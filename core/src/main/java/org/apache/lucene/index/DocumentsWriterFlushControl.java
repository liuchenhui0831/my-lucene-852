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
package org.apache.lucene.index;


import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during
 * indexing. It tracks the memory consumption per
 * {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy} to
 * decide if a {@link DocumentsWriterPerThread} must flush.
 * <p>
 * In addition to the {@link FlushPolicy} the flush control might set certain
 * {@link DocumentsWriterPerThread} as flush pending iff a
 * {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address
 * space exhaustion.
 *
 * 定义了ThreadState在添加/更新文档过程中的各种行为
 *
 * 多线程（持有相同的IndexWriter对象的引用）执行添加/更新操作时，每个线程都会获取一个ThreadState，
 * 每次执行完一次添加/更新的后，如果持有的DWPT对象收集的索引信息没有达到flush的要求，该索引信息的
 * 大小会被累加到activeBytes，否则会被累加到flushBytes中，并且执行flush操作，这种方式即 添加/更新
 * 和flush为并行操作。
 */
final class DocumentsWriterFlushControl implements Accountable {

  private final long hardMaxBytesPerDWPT;
  //多线程（持有相同的IndexWriter对象的引用）执行添加/更新操作时，每一个DWPT收集到的IndexByteUsed都会被累加到activeBytes中
  private long activeBytes = 0;
  //待写入到磁盘的索引数据量，如果全局的flush被触发，即使某个ThreadState中的DWPT达不到flush的要求，DWPT中的索引信息
  //   也会被累加到flushBytes中(没有触发全局flush的话，则是被累加到activeBytes中)
  private volatile long flushBytes = 0;
  // 描述了被标记为flushPending的ThreadState的个数
  private volatile int numPending = 0;
  private int numDocsSinceStalled = 0; // only with assert
  /**
   * 每当将删除信息添加到DeleteQueue后，如果DeleteQueue中的删除信息
   * 使用的内存量超过ramBufferSizeMB，flushDeletes会被置为true。
   */
  final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  // 全局flush是否被触发的标志
  private boolean fullFlush = false;
  /**待执行flush的DWPT的集合
   *
   *  fullFlushBuffer、blockedFlushes、flushingWriters、flushQueue之间的关联:
   *  当全局flush触发，fullFlushBuffer跟blockedFlushes中DWPT都会被添加进flushQueue，
   *  触发全局flush的线程总是只从flushQueue中依次取出每一个DWPT，当执行完doFlush()的
   *  操作后，将该DWPT占用的内存大小从flushingWriters中移除.
   */
  private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();
  // only for safety reasons if a DWPT is close to the RAM limit
  /**
   * 线程获得了ThreadState的锁，直到处理完文档后才会释放锁。如果在处理文档期间（未释放锁），
   * 有其他线程触发了全局的flush，并且ThreadState中持有的DWPT对象达到了flush的条件,
   * 那么该DWPT会被添加到blockedFlushes中，并且在blockedFlushes中的DWPT优先fullFlushBuffer中
   * 的所有DWPT去执行doFlush
   *
   * 目的是为了能保证blockedFlushes中的DWPT能优先添加到flushQueue中
   */
  private final Queue<BlockedFlush> blockedFlushes = new LinkedList<>();
  /**
   * [DWPT, DWPT收集的所有信息占用的内存大小]
   *  元素个数就是当前待flush的DWPT的个数，所有value的和值描述了当前内存中的flushBytes。
   *  IdentityHashMap同HashMap,区别是普通的HashMap的key通过.equal比较，但是这个用的==
   */
  private final IdentityHashMap<DocumentsWriterPerThread, Long> flushingWriters = new IdentityHashMap<>();


  double maxConfiguredRamBuffer = 0;
  long peakActiveBytes = 0;// only with assert
  long peakFlushBytes = 0;// only with assert
  long peakNetBytes = 0;// only with assert
  long peakDelta = 0; // only with assert
  boolean flushByRAMWasDisabled; // only with assert
  final DocumentsWriterStallControl stallControl;
  private final DocumentsWriterPerThreadPool perThreadPool;
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final DocumentsWriter documentsWriter;
  private final LiveIndexWriterConfig config;
  private final InfoStream infoStream;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter, LiveIndexWriterConfig config) {
    this.infoStream = config.getInfoStream();
    this.stallControl = new DocumentsWriterStallControl();
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = documentsWriter.flushPolicy;
    this.config = config;
    this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024;
    this.documentsWriter = documentsWriter;
  }

  public synchronized long activeBytes() {
    return activeBytes;
  }

  public long getFlushingBytes() {
    return flushBytes;
  }

  public synchronized long netBytes() {
    return flushBytes + activeBytes;
  }
  
  private long stallLimitBytes() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH ? (long)(2 * (maxRamMB * 1024 * 1024)) : Long.MAX_VALUE;
  }
  
  private boolean assertMemory() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    // We can only assert if we have always been flushing by RAM usage; otherwise the assert will false trip if e.g. the
    // flush-by-doc-count * doc size was large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
    if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && flushByRAMWasDisabled == false) {
      // for this assert we must be tolerant to ram buffer changes!
      maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
      final long ram = flushBytes + activeBytes;
      final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
      // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had maxMem and the last doc had the peakDelta
      
      // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this is still a valid limit
      // (numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) -> those are the total number of DWPT that are not active but not yet fully flushed
      // all of them could theoretically be taken out of the loop once they crossed the RAM buffer and the last document was the peak delta
      // (numDocsSinceStalled * peakDelta) -> at any given time there could be n threads in flight that crossed the stall control before we reached the limit and each of them could hold a peak document
      final long expected = (2 * ramBufferBytes) + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) + (numDocsSinceStalled * peakDelta);
      // the expected ram consumption is an upper bound at this point and not really the expected consumption
      if (peakDelta < (ramBufferBytes >> 1)) {
        /*
         * if we are indexing with very low maxRamBuffer like 0.1MB memory can
         * easily overflow if we check out some DWPT based on docCount and have
         * several DWPT in flight indexing large documents (compared to the ram
         * buffer). This means that those DWPT and their threads will not hit
         * the stall control before asserting the memory which would in turn
         * fail. To prevent this we only assert if the the largest document seen
         * is smaller than the 1/2 of the maxRamBufferMB
         */
        assert ram <= expected : "actual mem: " + ram + " byte, expected mem: " + expected
            + " byte, flush mem: " + flushBytes + ", active mem: " + activeBytes
            + ", pending DWPT: " + numPending + ", flushing DWPT: "
            + numFlushingDWPT() + ", blocked DWPT: " + numBlockedFlushes()
            + ", peakDelta mem: " + peakDelta + " bytes, ramBufferBytes=" + ramBufferBytes
            + ", maxConfiguredRamBuffer=" + maxConfiguredRamBuffer;
      }
    } else {
      flushByRAMWasDisabled = true;
    }
    return true;
  }

  /**
   * 描述了刚刚完成添加/更新的DWPT收集到的索引信息应该被添加到activeBytes还是flushBytes中，取决于ThreadState的flushPending状态
   * @param perThread
   */
  private void commitPerThreadBytes(ThreadState perThread) {
    final long delta = perThread.dwpt.bytesUsed() - perThread.bytesUsed;
    perThread.bytesUsed += delta;
    /*
     * We need to differentiate here if we are pending since setFlushPending
     * moves the perThread memory to the flushBytes and we could be set to
     * pending during a delete
     */
    if (perThread.flushPending) {
      flushBytes += delta;
    } else {
      activeBytes += delta;
    }
    assert updatePeaks(delta);
  }

  // only for asserts
  private boolean updatePeaks(long delta) {
    peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
    peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
    peakNetBytes = Math.max(peakNetBytes, netBytes());
    peakDelta = Math.max(peakDelta, delta);
    
    return true;
  }

  synchronized DocumentsWriterPerThread doAfterDocument(ThreadState perThread, boolean isUpdate) {
    try {
      commitPerThreadBytes(perThread);
      if (!perThread.flushPending) {
        if (isUpdate) {
          flushPolicy.onUpdate(this, perThread);
        } else {
          flushPolicy.onInsert(this, perThread);
        }
        if (!perThread.flushPending && perThread.bytesUsed > hardMaxBytesPerDWPT) {
          // Safety check to prevent a single DWPT exceeding its RAM limit. This
          // is super important since we can not address more than 2048 MB per DWPT
          setFlushPending(perThread);
        }
      }
      return checkout(perThread, false);
    } finally {
      boolean stalled = updateStallState();
      assert assertNumDocsSinceStalled(stalled) && assertMemory();
    }
  }

  private DocumentsWriterPerThread checkout(ThreadState perThread, boolean markPending) {
    if (fullFlush) {
      if (perThread.flushPending) {
        checkoutAndBlock(perThread);
        return nextPendingFlush();
      } else {
        return null;
      }
    } else {
      if (markPending) {
        assert perThread.isFlushPending() == false;
        setFlushPending(perThread);
      }
      return tryCheckoutForFlush(perThread);
    }
  }
  
  private boolean assertNumDocsSinceStalled(boolean stalled) {
    /*
     *  updates the number of documents "finished" while we are in a stalled state.
     *  this is important for asserting memory upper bounds since it corresponds 
     *  to the number of threads that are in-flight and crossed the stall control
     *  check before we actually stalled.
     *  see #assertMemory()
     */
    if (stalled) { 
      numDocsSinceStalled++;
    } else {
      numDocsSinceStalled = 0;
    }
    return true;
  }

  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.containsKey(dwpt);
    try {
      Long bytes = flushingWriters.remove(dwpt);
      flushBytes -= bytes.longValue();
      perThreadPool.recycle(dwpt);
      assert assertMemory();
    } finally {
      try {
        updateStallState();
      } finally {
        notifyAll();
      }
    }
  }

  private long stallStartNS;

  /**
    添加、更新文档和主动flush是并行操作，可能会降低健康度。当达到阈值时，会阻塞这些
    add/update操作的线程。flush操作会提高健康度，执行完flush后，如果健康度恢复到小于
    阈值，就会唤醒那些被阻塞的线程。阈值的判断公式涉及的变量的变化就需要调用这个方法。

    满足条件时需要阻塞添加/更新操作
    (activeBytes+flushBytes) > limit && activeBytes < limit
    limit=2*ramBufferSizeMB, ramBufferSizeMB描述了索引信息被写入到磁盘前暂存在内存
    中允许的最大使用内存
   */
  private boolean updateStallState() {
    
    assert Thread.holdsLock(this);
    final long limit = stallLimitBytes();
    /*
     * we block indexing threads if net byte grows due to slow flushes
     * yet, for small ram buffers and large documents we can easily
     * reach the limit without any ongoing flushes. we need to ensure
     * that we don't stall/block if an ongoing or pending flush can
     * not free up enough memory to release the stall lock.
     */
    final boolean stall = (activeBytes + flushBytes) > limit &&
      activeBytes < limit &&
      !closed;

    if (infoStream.isEnabled("DWFC")) {
      if (stall != stallControl.anyStalledThreads()) {
        if (stall) {
          infoStream.message("DW", String.format(Locale.ROOT, "now stalling flushes: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                                                 netBytes()/1024./1024., getFlushingBytes()/1024./1024., fullFlush));
          stallStartNS = System.nanoTime();
        } else {
          infoStream.message("DW", String.format(Locale.ROOT, "done stalling flushes for %.1f msec: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                                                 (System.nanoTime()-stallStartNS)/1000000., netBytes()/1024./1024., getFlushingBytes()/1024./1024., fullFlush));
        }
      }
    }

    stallControl.updateStalled(stall);
    return stall;
  }
  
  public synchronized void waitForFlush() {
    while (flushingWriters.size() != 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }

  /**
   * Sets flush pending state on the given {@link ThreadState}. The
   * {@link ThreadState} must have indexed at least on Document and must not be
   * already pending.
   *
   * 将ThreadState状态设置为flushPending，从activeBytes中减去索引量，然后将它加到flushBytes中，
   * 两个值从而影响flush健康状况
   */
  public synchronized void setFlushPending(ThreadState perThread) {
    assert !perThread.flushPending;
    if (perThread.dwpt.getNumDocsInRAM() > 0) {
      perThread.flushPending = true; // write access synced
      final long bytes = perThread.bytesUsed;
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
      assert assertMemory();
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing
    
  }
  
  synchronized void doOnAbort(ThreadState state) {
    try {
      if (state.flushPending) {
        flushBytes -= state.bytesUsed;
      } else {
        activeBytes -= state.bytesUsed;
      }
      assert assertMemory();
      // Take it out of the loop this DWPT is stale
      perThreadPool.reset(state);
    } finally {
      updateStallState();
    }
  }

  synchronized DocumentsWriterPerThread tryCheckoutForFlush(
      ThreadState perThread) {
   return perThread.flushPending ? internalTryCheckOutForFlush(perThread) : null;
  }
  
  private void checkoutAndBlock(ThreadState perThread) {
    perThread.lock();
    try {
      assert perThread.flushPending : "can not block non-pending threadstate";
      assert fullFlush : "can not block if fullFlush == false";
      final DocumentsWriterPerThread dwpt;
      final long bytes = perThread.bytesUsed;
      dwpt = perThreadPool.reset(perThread);
      numPending--;
      blockedFlushes.add(new BlockedFlush(dwpt, bytes));
    } finally {
      perThread.unlock();
    }
  }

  private DocumentsWriterPerThread internalTryCheckOutForFlush(ThreadState perThread) {
    assert Thread.holdsLock(this);
    assert perThread.flushPending;
    try {
      // We are pending so all memory is already moved to flushBytes
      if (perThread.tryLock()) {
        try {
          if (perThread.isInitialized()) {
            assert perThread.isHeldByCurrentThread();
            final DocumentsWriterPerThread dwpt;
            final long bytes = perThread.bytesUsed; // do that before
                                                         // replace!
            dwpt = perThreadPool.reset(perThread);
            assert !flushingWriters.containsKey(dwpt) : "DWPT is already flushing";
            // Record the flushing DWPT to reduce flushBytes in doAfterFlush
            flushingWriters.put(dwpt, Long.valueOf(bytes));
            numPending--; // write access synced
            return dwpt;
          }
        } finally {
          perThread.unlock();
        }
      }
      return null;
    } finally {
      updateStallState();
    }
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
        + ", flushBytes=" + flushBytes + "]";
  }

  DocumentsWriterPerThread nextPendingFlush() {
    int numPending;
    boolean fullFlush;
    synchronized (this) {
      final DocumentsWriterPerThread poll;
      if ((poll = flushQueue.poll()) != null) {
        updateStallState();
        return poll;
      }
      fullFlush = this.fullFlush;
      numPending = this.numPending;
    }
    if (numPending > 0 && !fullFlush) { // don't check if we are doing a full flush
      final int limit = perThreadPool.getActiveThreadStateCount();
      for (int i = 0; i < limit && numPending > 0; i++) {
        final ThreadState next = perThreadPool.getThreadState(i);
        if (next.flushPending) {
          final DocumentsWriterPerThread dwpt = tryCheckoutForFlush(next);
          if (dwpt != null) {
            return dwpt;
          }
        }
      }
    }
    return null;
  }

  synchronized void setClosed() {
    // set by DW to signal that we should not release new DWPT after close
    this.closed = true;
  }

  /**
   * Returns an iterator that provides access to all currently active {@link ThreadState}s 
   */
  public Iterator<ThreadState> allActiveThreadStates() {
    return getPerThreadsIterator(perThreadPool.getActiveThreadStateCount());
  }
  
  private Iterator<ThreadState> getPerThreadsIterator(final int upto) {
    return new Iterator<ThreadState>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return i < upto;
      }

      @Override
      public ThreadState next() {
        return perThreadPool.getThreadState(i++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onDelete(this, null);
  }

  /** Returns heap bytes currently consumed by buffered deletes/updates that would be
   *  freed if we pushed all deletes.  This does not include bytes consumed by
   *  already pushed delete/update packets. */
  public long getDeleteBytesUsed() {
    return documentsWriter.deleteQueue.ramBytesUsed();
  }

  @Override
  public long ramBytesUsed() {
    // TODO: improve this to return more detailed info?
    return getDeleteBytesUsed() + netBytes();
  }
  
  synchronized int numFlushingDWPT() {
    return flushingWriters.size();
  }
  
  public boolean getAndResetApplyAllDeletes() {
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {
    flushDeletes.set(true);
  }
  
  ThreadState obtainAndLock() {
    final ThreadState perThread = perThreadPool.getAndLock();
    boolean success = false;
    try {
      if (perThread.isInitialized() && perThread.dwpt.deleteQueue != documentsWriter.deleteQueue) {
        // There is a flush-all in process and this DWPT is
        // now stale -- enroll it for flush and try for
        // another DWPT:
        addFlushableState(perThread);
      }
      success = true;
      // simply return the ThreadState even in a flush all case sine we already hold the lock
      return perThread;
    } finally {
      if (!success) { // make sure we unlock if this fails
        perThreadPool.release(perThread);
      }
    }
  }
  
  long markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    long seqNo;
    synchronized (this) {
      assert !fullFlush : "called DWFC#markForFullFlush() while full flush is still running";
      assert fullFlushBuffer.isEmpty() : "full flush buffer should be empty: "+ fullFlushBuffer;
      fullFlush = true; //表示当前线程正在执行主动flush操作，fullFLush的作用范围包括正在执行添加/更新、删除的其他线程
      //持有这个flushQueue的DWPT都是这在这次要flush的作用范围内，如果不是的话，说明是用的下面的newQueue,最多只会存在2个flushQueue.
      flushingQueue = documentsWriter.deleteQueue;
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush

      perThreadPool.lockNewThreadStates(); // no new thread-states while we do a flush otherwise the seqNo accounting might be off
      try {
        // Insert a gap in seqNo of current active thread count, in the worst case each of those threads now have one operation in flight.  It's fine
        // if we have some sequence numbers that were never assigned:
        seqNo = documentsWriter.deleteQueue.getLastSequenceNumber() + perThreadPool.getActiveThreadStateCount() + 2;
        flushingQueue.maxSeqNo = seqNo + 1;
        DocumentsWriterDeleteQueue newQueue = new DocumentsWriterDeleteQueue(infoStream, flushingQueue.generation + 1, seqNo + 1);
        documentsWriter.deleteQueue = newQueue; //用新的DeleteQueue替换

      } finally {
        perThreadPool.unlockNewThreadStates();
      }
    }
    // DWPTP中还有没有ThreadState
    final int limit = perThreadPool.getActiveThreadStateCount();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        if (!next.isInitialized()) {
          continue; 
        }
        assert next.dwpt.deleteQueue == flushingQueue
            || next.dwpt.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
            + flushingQueue
            + " currentqueue: "
            + documentsWriter.deleteQueue
            + " perThread queue: "
            + next.dwpt.deleteQueue
            + " numDocsInRam: " + next.dwpt.getNumDocsInRAM();
        if (next.dwpt.deleteQueue != flushingQueue) { //不是旧得那个flushingQueue
          // this one is already a new DWPT
          continue;
        }
        addFlushableState(next); //设置ThreadState状态，然后添加到队列中
      } finally {
        next.unlock();
      }
    }
    synchronized (this) {
      /* make sure we move all DWPT that are where concurrently marked as
       * pending and moved to blocked are moved over to the flushQueue. There is
       * a chance that this happens since we marking DWPT for full flush without
       * blocking indexing.*/
      // 将blockedFlushes中的DWPT添加到flushQueue中
      pruneBlockedQueue(flushingQueue); //传入那个旧的flushingQueue
      assert assertBlockedFlushes(documentsWriter.deleteQueue);
      flushQueue.addAll(fullFlushBuffer);
      fullFlushBuffer.clear();
      updateStallState();
    }
    assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
    return seqNo;
  }
  
  private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
    final int limit = perThreadPool.getActiveThreadStateCount();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        assert !next.isInitialized() || next.dwpt.deleteQueue == queue : "isInitialized: " + next.isInitialized() + " numDocs: " + (next.isInitialized() ? next.dwpt.getNumDocsInRAM() : 0) ;
      } finally {
        next.unlock();
      }
    }
    return true;
  }
  /**
   * 当全局flush触发时，意味着该flush之前所有添加的文档都在该flush的作用范围内，
   * 那么需要将DWPTP中所有持有DWPT对象引用，并且DWPT的私有变量numDocsInRAM 需要大于 0
   * 即该DWPT处理过文档的所有ThreadState置为flushPending，他们所持有的DWPT随后将其
   * 收集到的索引信息各自生成一个段，在此之前DWPT先存放到fullFlushBuffer链表中。
   */
  private final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();

  void addFlushableState(ThreadState perThread) {
    if (infoStream.isEnabled("DWFC")) {
      infoStream.message("DWFC", "addFlushableState " + perThread.dwpt);
    }
    final DocumentsWriterPerThread dwpt = perThread.dwpt;
    assert perThread.isHeldByCurrentThread();
    assert perThread.isInitialized();
    assert fullFlush;
    assert dwpt.deleteQueue != documentsWriter.deleteQueue;
    if (dwpt.getNumDocsInRAM() > 0) { //numDocsInRam的个数描述了DWPT处理的文档数
      synchronized(this) {
        if (!perThread.flushPending) {
          setFlushPending(perThread); //设置ThreadState为flushPending状态
        }
        final DocumentsWriterPerThread flushingDWPT = internalTryCheckOutForFlush(perThread);
        assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
        assert dwpt == flushingDWPT : "flushControl returned different DWPT";
        fullFlushBuffer.add(flushingDWPT);
      }
    } else {
      //重置ThreadState状态为!flushPending，并取消置关联的DWPT为null
      perThreadPool.reset(perThread); // make this state inactive
    }
  }
  
  /**
   * Prunes the blockedQueue by removing all DWPT that are associated with the given flush queue.
   * 将blockedFlushes中的DWPT添加到flushQueue中
   */
  private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
    Iterator<BlockedFlush> iterator = blockedFlushes.iterator();
    while (iterator.hasNext()) {
      BlockedFlush blockedFlush = iterator.next();
      if (blockedFlush.dwpt.deleteQueue == flushingQueue) { //用的旧的flushingQueue的DWPT，在此次范围内
        iterator.remove();
        assert !flushingWriters.containsKey(blockedFlush.dwpt) : "DWPT is already flushing";
        // Record the flushing DWPT to reduce flushBytes in doAfterFlush
        flushingWriters.put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
        // don't decr pending here - it's already done when DWPT is blocked
        flushQueue.add(blockedFlush.dwpt);
      }
    }
  }
  
  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    assert flushingWriters.isEmpty();
    try {
      //从blockedFlushes中将newQueue对应的DWPT添加到flushQueue中，
      //如果在主动flush期间，其他线程添加/更新的文档操作满足自动flush的条件，对应的DWPT会被暂时存放
      //在blockedFLushes中。
      if (!blockedFlushes.isEmpty()) {
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        pruneBlockedQueue(documentsWriter.deleteQueue); // prune 删除，减少
        assert blockedFlushes.isEmpty();
      }
    } finally {
      fullFlush = false;
      updateStallState();
    }
  }
  
  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    for (BlockedFlush blockedFlush : blockedFlushes) {
      assert blockedFlush.dwpt.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
   try {
     abortPendingFlushes();
   } finally {
     fullFlush = false;
   }
  }
  
  synchronized void abortPendingFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        try {
          documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
          dwpt.abort();
        } catch (Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(dwpt);
        }
      }
      for (BlockedFlush blockedFlush : blockedFlushes) {
        try {
          flushingWriters.put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
          documentsWriter.subtractFlushedNumDocs(blockedFlush.dwpt.getNumDocsInRAM());
          blockedFlush.dwpt.abort();
        } catch (Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(blockedFlush.dwpt);
        }
      }
    } finally {
      flushQueue.clear();
      blockedFlushes.clear();
      updateStallState();
    }
  }
  
  /**
   * Returns <code>true</code> if a full flush is currently running
   */
  synchronized boolean isFullFlush() {
    return fullFlush;
  }

  /**
   * Returns the number of flushes that are already checked out but not yet
   * actively flushing
   */
  synchronized int numQueuedFlushes() {
    return flushQueue.size();
  }

  /**
   * Returns the number of flushes that are checked out but not yet available
   * for flushing. This only applies during a full flush if a DWPT needs
   * flushing but must not be flushed until the full flush has finished.
   */
  synchronized int numBlockedFlushes() {
    return blockedFlushes.size();
  }
  
  private static class BlockedFlush {
    final DocumentsWriterPerThread dwpt;
    final long bytes;
    BlockedFlush(DocumentsWriterPerThread dwpt, long bytes) {
      super();
      this.dwpt = dwpt;
      this.bytes = bytes;
    }
  }

  /**
   * This method will block if too many DWPT are currently flushing and no
   * checked out DWPT are available
   */
  void waitIfStalled() {
    stallControl.waitIfStalled();
  }

  /**
   * Returns <code>true</code> iff stalled
   */
  boolean anyStalledThreads() {
    return stallControl.anyStalledThreads();
  }
  
  /**
   * Returns the {@link IndexWriter} {@link InfoStream}
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }

  synchronized ThreadState findLargestNonPendingWriter() {
    ThreadState maxRamUsingThreadState = null;
    long maxRamSoFar = 0;
    Iterator<ThreadState> activePerThreadsIterator = allActiveThreadStates();
    int count = 0;
    while (activePerThreadsIterator.hasNext()) {
      ThreadState next = activePerThreadsIterator.next();
      if (!next.flushPending) {
        final long nextRam = next.bytesUsed;
        if (nextRam > 0 && next.dwpt.getNumDocsInRAM() > 0) {
          if (infoStream.isEnabled("FP")) {
            infoStream.message("FP", "thread state has " + nextRam + " bytes; docInRAM=" + next.dwpt.getNumDocsInRAM());
          }
          count++;
          if (nextRam > maxRamSoFar) {
            maxRamSoFar = nextRam;
            maxRamUsingThreadState = next;
          }
        }
      }
    }
    if (infoStream.isEnabled("FP")) {
      infoStream.message("FP", count + " in-use non-flushing threads states");
    }
    return maxRamUsingThreadState;
  }

  /**
   * Returns the largest non-pending flushable DWPT or <code>null</code> if there is none.
   */
  final DocumentsWriterPerThread checkoutLargestNonPendingWriter() {
    ThreadState largestNonPendingWriter = findLargestNonPendingWriter();
    if (largestNonPendingWriter != null) {
      // we only lock this very briefly to swap it's DWPT out - we don't go through the DWPTPool and it's free queue
      largestNonPendingWriter.lock();
      try {
        synchronized (this) {
          try {
            if (largestNonPendingWriter.isInitialized() == false) {
              return nextPendingFlush();
            } else {
              return checkout(largestNonPendingWriter, largestNonPendingWriter.isFlushPending() == false);
            }
          } finally {
            updateStallState();
          }
        }
      } finally {
        largestNonPendingWriter.unlock();
      }
    }
    return null;
  }
}
