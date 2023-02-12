/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;


import java.util.Arrays;

/**
 * A pool for int blocks similar to {@link ByteBlockPool}
 * @lucene.internal
 */
public final class IntBlockPool {
  public final static int INT_BLOCK_SHIFT = 13;
  // 一维数组的大小。
  public final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  public final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;
  
  /** Abstract class for allocating and freeing int
   *  blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    public Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleIntBlocks(int[][] blocks, int start, int end);

    public int[] getIntBlock() {
      return new int[blockSize];
    }
  }
  
  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    /**
     * Creates a new {@link DirectAllocator} with a default block size
     */
    public DirectAllocator() {
      super(INT_BLOCK_SIZE);
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int start, int end) {
    }
  }
  
  /** array of buffers currently used in the pool. Buffers are allocated if needed don't modify this outside of this class */
  // 存储term的位置，词频，payload，默认是由10个大小为 INT_BLOCK_SIZE大小的一维数组组成的。
  public int[][] buffers = new int[10][];

  /** index into the buffers array pointing to the current buffer used as the head */
  // 用来表示我们正在使用第几个一维数组(前面定义了一个包含10个一维数组的二维数组)。
  private int bufferUpto = -1;   
  /** Pointer to the current position in head buffer */
  // 用来表示一维数组中可以使用的位置(偏移)。
  public int intUpto = INT_BLOCK_SIZE;
  /** Current head buffer */
  // 同一时刻只选取二维数组中的一个一维数组进行数据的存储跟读取，buffer用来表示我们正在使用的那个一维数组。
  // 这个buffer又称为 head buffer。
  public int[] buffer;
  /** Current head offset */
  // 用来表示在二维数组中可以使用的位置(偏移)。
  public int intOffset = -INT_BLOCK_SIZE;

  private final Allocator allocator;

  /**
   * Creates a new {@link IntBlockPool} with a default {@link Allocator}.
   * @see IntBlockPool#nextBuffer()
   */
  public IntBlockPool() {
    this(new DirectAllocator());
  }
  
  /**
   * Creates a new {@link IntBlockPool} with the given {@link Allocator}.
   * @see IntBlockPool#nextBuffer()
   */
  public IntBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }
  
  /**
   * Resets the pool to its initial state reusing the first buffer. Calling
   * {@link IntBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    this.reset(true, true);
  }
  
  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. 
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <tt>0</tt>. 
   *        This should be set to <code>true</code> if this pool is used with 
   *        {@link SliceWriter}.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling
   *        {@link IntBlockPool#nextBuffer()} is not needed after reset iff the 
   *        block pool was used before ie. {@link IntBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for(int i=0;i<bufferUpto;i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, intUpto, 0);
      }
     
      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;  
        // Recycle all but the first buffer
        allocator.recycleIntBlocks(buffers, offset, 1+bufferUpto);
        Arrays.fill(buffers, offset, bufferUpto+1, null);
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        intUpto = 0;
        intOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        intUpto = INT_BLOCK_SIZE;
        intOffset = -INT_BLOCK_SIZE;
        buffer = null;
      }
    }
  }
  
  /**
   * Advances the pool to its next buffer. This method should be called once
   * after the constructor to initialize the pool. In contrast to the
   * constructor a {@link IntBlockPool#reset()} call will advance the pool to
   * its first buffer immediately.
   */
  public void nextBuffer() {
    if (1+bufferUpto == buffers.length) {
      //二维数组满了，扩容
      int[][] newBuffers = new int[(int) (buffers.length*1.5)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers;
    }
    buffer = buffers[1+bufferUpto] = allocator.getIntBlock(); //生成新的一维数组
    bufferUpto++;  //修改当前使用的第几个一维数组

    intUpto = 0;  //可使用位置置为0
    intOffset += INT_BLOCK_SIZE; //更新二维数组可使用位置
  }
  
  /**
   * Creates a new int slice with the given starting size and returns the slices offset in the pool.
   * @see SliceReader
   */
  private int newSlice(final int size) {
    if (intUpto > INT_BLOCK_SIZE-size) {
      nextBuffer();  //空间不够，分配一个新的一维数组
      assert assertSliceBuffer(buffer);
    }
      
    final int upto = intUpto;
    intUpto += size;
    buffer[intUpto-1] = 1; //指定下次分片的级别
    return upto;
  }
  
  private static final boolean assertSliceBuffer(int[] buffer) {
    int count = 0;
    for (int i = 0; i < buffer.length; i++) {
      count += buffer[i]; // for slices the buffer must only have 0 values
    }
    return count == 0;
  }
  
  
  // no need to make this public unless we support different sizes
  // TODO make the levels and the sizes configurable
  /**
   * An array holding the offset into the {@link IntBlockPool#LEVEL_SIZE_ARRAY}
   * to quickly navigate to the next slice level.
   */
  // 分层的级别，代表下次分配新的分片(slice)的级别(层级)，数组元素越大，分片大小越大，数组元素用来作为LEVEL_SIZE_ARRAY[]数组的下标值。
  private final static int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
  
  /**
   * An array holding the level sizes for int slices.
   */
  // 数组元素表示新的分片的大小。
  private final static int[] LEVEL_SIZE_ARRAY = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
  
  /**
   * The first level size for new slices
   */
  private final static int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  /**
   * Allocates a new slice from the given offset
   */
  private int allocSlice(final int[] slice, final int sliceOffset) {
    final int level = slice[sliceOffset]; //获取层级
    final int newLevel = NEXT_LEVEL_ARRAY[level-1];
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];
    // Maybe allocate another block
    if (intUpto > INT_BLOCK_SIZE-newSize) {
      nextBuffer();
      assert assertSliceBuffer(buffer);
    }

    final int newUpto = intUpto;
    final int offset = newUpto + intOffset; //记录在二维数组中的位置
    intUpto += newSize; //更新head buffer中下一个可以使用的位置
    // Write forwarding address at end of last slice:
    //将sliceOffset位置的数组替换为offset，目的就是在读取数据时，被告知应该跳转到数组的哪一个位置继续
    //找这个term的其他偏移值(在文本中的偏移量)跟payload值。
    //换句话如果一篇文档的某个term出现多次，那么记录这个term的在文本中的所有偏移值跟payload并不是连续存储的。
    slice[sliceOffset] = offset;
        
    // Write new level:
    buffer[intUpto-1] = newLevel;

    return newUpto;
  }
  
  /**
   * A {@link SliceWriter} that allows to write multiple integer slices into a given {@link IntBlockPool}.
   * 
   *  @see SliceReader
   *  @lucene.internal
   */
  public static class SliceWriter {
    /**描述在二维数组中的位置*/
    private int offset;
    private final IntBlockPool pool;
    
    
    public SliceWriter(IntBlockPool pool) {
      this.pool = pool;
    }
    /**
     * 写入的域值跟上一个处理的域值不一样，并且已经在分片存储过，
     * 那么写入之前需要调用下面方法另offset的值为这个域值上次在分片中的结束位置
     */
    public void reset(int sliceOffset) {
      this.offset = sliceOffset;
    }
    
    /**
     * Writes the given value into the slice and resizes the slice if needed
     */
    public void writeInt(int value) {
      // 获得head buffer这个一维数组, offset >> INT_BLOCK_SHIFT的值就是head buffer在二维数组中的行数。
      int[] ints = pool.buffers[offset >> INT_BLOCK_SHIFT];
      assert ints != null;
      int relativeOffset = offset & INT_BLOCK_MASK; // 获得在head buffer这个一维数组组内的偏移。&低13位
      if (ints[relativeOffset] != 0) { //当前位置存放的是分片的层级
        // End of slice; allocate a new one
          relativeOffset = pool.allocSlice(ints, relativeOffset);
        ints = pool.buffer;  //alloc后，headBuffer发生了变化
        offset = relativeOffset + pool.intOffset;
      }
      ints[relativeOffset] = value;
      offset++; 
    }
    
    /**
     * starts a new slice and returns the start offset. The returned value
     * should be used as the start offset to initialize a {@link SliceReader}.
     */
    //存储一个新的域值时候需要调用下面的方法来分配一个分片(仅在MemoryIndex中会调用此方法
    public int startNewSlice() {
      return offset = pool.newSlice(FIRST_LEVEL_SIZE) + pool.intOffset;
      
    }
    
    /**
     * Returns the offset of the currently written slice. The returned value
     * should be used as the end offset to initialize a {@link SliceReader} once
     * this slice is fully written or to reset the this writer if another slice
     * needs to be written.
     */
    public int getCurrentOffset() {
      return offset;
    }
  }
  
  /**
   * A {@link SliceReader} that can read int slices written by a {@link SliceWriter}
   * @lucene.internal
   */
  public static final class SliceReader {
    
    private final IntBlockPool pool;
    private int upto;  // 当前读取的位置
    private int bufferUpto;  // 指定了二维数组的某个一维数组。
    private int bufferOffset;   // 一维数组的第一个元素在二维数组中的偏移量(位置)。
    private int[] buffer;   // 当前正在读取数据的一维数组。
    private int limit;   // limit作为下标描述了下一个存储当前term信息的位置。
    private int level;  // 获得分片的层级。
    private int end;  // 这个term的最后一个信息的位置。
    
    /**
     * Creates a new {@link SliceReader} on the given pool
     */
    public SliceReader(IntBlockPool pool) {
      this.pool = pool;
    }

    /**
     * Resets the reader to a slice give the slices absolute start and end offset in the pool
     */
    public void reset(int startOffset, int endOffset) {
      bufferUpto = startOffset / INT_BLOCK_SIZE;
      bufferOffset = bufferUpto * INT_BLOCK_SIZE;
      this.end = endOffset;
      upto = startOffset;
      level = 1;
      
      buffer = pool.buffers[bufferUpto];
      upto = startOffset & INT_BLOCK_MASK;

      final int firstSize = IntBlockPool.LEVEL_SIZE_ARRAY[0];
      if (startOffset+firstSize >= endOffset) {
        // There is only this one slice to read
        limit = endOffset & INT_BLOCK_MASK;
      } else {
        limit = upto+firstSize-1;
      }

    }
    
    /**
     * Returns <code>true</code> iff the current slice is fully read. If this
     * method returns <code>true</code> {@link SliceReader#readInt()} should not
     * be called again on this slice.
     */
    public boolean endOfSlice() {
      assert upto + bufferOffset <= end;
      return upto + bufferOffset == end;
    }
    
    /**
     * Reads the next int from the current slice and returns it.
     * @see SliceReader#endOfSlice()
     */
    public int readInt() {
      assert !endOfSlice();
      assert upto <= limit;
      if (upto == limit)
        nextSlice();
      return buffer[upto++];
    }
    
    private void nextSlice() {
      // Skip to our next slice
      final int nextIndex = buffer[limit];  // 找到下一个存储term信息的位置。
      level = NEXT_LEVEL_ARRAY[level-1];
      final int newSize = LEVEL_SIZE_ARRAY[level];  // 获得分片的大小

      bufferUpto = nextIndex / INT_BLOCK_SIZE;  // 计算出在二维数组中的第几层。
      bufferOffset = bufferUpto * INT_BLOCK_SIZE;  // 当前的一维数组的第一个元素在二维数组中的位置。

      buffer = pool.buffers[bufferUpto]; // 取出 head buffer。
      upto = nextIndex & INT_BLOCK_MASK; // 计算出在head buffer中的起始位置。

      if (nextIndex + newSize >= end) {   // 更新limit的值。
        // We are advancing to the final slice
        assert end - nextIndex > 0;
        limit = end - bufferOffset;
      } else {
        // This is not the final slice (subtract 4 for the
        // forwarding address at the end of this new slice)
        limit = upto+newSize-1;  // 还有其他的slice没有读取到, 将limit的值置为下一个slice的起始位置。
      }
    }
  }
}

