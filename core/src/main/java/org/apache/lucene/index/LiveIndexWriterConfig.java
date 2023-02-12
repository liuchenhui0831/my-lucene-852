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


import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

/**
 * 不可变配置
 * - OpenMode： 描述了在IndexWriter的初始化阶段，如何处理索引目录中的已有的索引文件，这里称之为旧的索引，OpenMode一共定义了
 *         三种模式，即：CREATE、APPEND、CREATE_OR_APPEND。
 * - IndexDeletionPolicy： 描述当一个新的提交生成后，如何处理上一个提交；
 * - IndexCommit：
 *         执行一次提交操作（执行commit方法）后，这次提交包含的所有的段的信息用IndexCommit来描述，其中至少包含了两个信息，
 *         分别是segment_N文件跟Directory。
 *
 *         索引删除策略SnapshotDeletionPolicy，在每次执行提交操作后，我们可以通过主动调用SnapshotDeletionPolicy.snapshot()来
 *         实现快照功能，而该方法的返回值就是IndexCommit。
 *
 *          如果设置了IndexCommit，那么在构造IndexWriter对象期间，会先读取IndexCommit中的索引信息，IndexCommit可以通
 *          过IndexWriterConfig.setIndexCommit(IndexCommit commit)方法设置，默认值为null。
 * - Similarity: 描述了Lucene打分的组成部分, 默认使用BM25。
 * - MergeScheduler: 段的合并调度策略，用来定义如何执行一个或多个段的合并，比如并发执行多个段的合并任务时的执行先后顺序，
 *        磁盘IO限制。默认使用ConcurrentMergeScheduler;
 * - Codec： 定义了索引文件的数据结构。默认使用Lucene84；
 * - DocumentsWriterPerThreadPool：逻辑上的线程池，它实现了类似Java线程池的功能。DocumentsWriterPerThreadPool中，
 *        每当IndexWriter要添加文档，会从DocumentsWriterPerThreadPool中获得一个ThreadState去执行，故在多线程（持有相同
 *        的IndexWriter对象引用）执行添加文档操作时，每个线程都会获得一个ThreadState对象；
 * - ReaderPooling：是一个布尔值，用来描述是否允许共用（pool）SegmentReader，共用（pool）可以理解为缓存，在第一次读取一个
 *        段的信息时，即获得该段对应的SegmentReader，并且使用ReaderPool来缓存这些SegmentReader，使得在处理删除信息（删除操
 *        作涉及多个段时效果更突出）、NRT搜索时可以提供更好的性能。默认为true；
 * - FlushPolicy: 即flush策略，准确的说应该称为 自动flush策略，因为flush分为自动flush跟主动flush，即显式调
 *        用IndexWriter.flush( )方法，FlushPolicy描述了IndexWriter执行了增删改的操作后，将修改后的索引信息写入磁盘的时机。
 *        默认为FlushByRamOrCountsPolicy；
 * - RAMPerThreadHardLimitMB： 被允许设置的值域为0~2048M，它用来描述每一个DWPT允许缓存的最大的索引量，默认1945；
 * - InfoStream： 进行调试时实现debug输出信息，在业务中打印debug信息会降低Lucene的性能，故在业务中使用默认值就行，即不输出debug信息；
 * - IndexSort：索引阶段如何对segment内的文档进行排序。默认为null；
 * - SoftDeletesField：定义哪些域为软删除的域，默认为null；
 *
 * 可变配置
 * - MergePolicy：段的合并策略，它用来描述如何从索引目录中找到满足合并要求的段集合。默认TieredMergePolicy；
 * - MaxBufferedDocs: 描述了索引信息被写入到磁盘前暂时缓存在内存中允许的文档最大数量，是一个DWPT允许添加的最大文档数量。默认-1；
 * - RAMBufferSizeMB: 描述了索引信息被写入到磁盘前暂时缓存在内存中允许的最大使用内存值。默认16；
 *      每次执行文档的增删改后，会调用FlushPolicy（flush策略）判断是否需要执行自动flush。flush策略FlushByRamOrCountsPolicy正是
 *      依据MaxBufferedDocs、RAMBufferSizeMB来判断是否需要执行自动flush。
 *
 *      如果每一个DWPT中的DocumentIndexData的个数超过MaxBufferedDocs时，那么就会触发自动flush，将DWPT中的索引信息生成为一个段，
 *      MaxBufferedDocs影响的是一个DWPT。
 *
 *      如果每一个DWPT中的所有DocumentIndexData的索引内存占用量超过RAMPerThreadHardLimitMB，那么就会触发自动flush，将DWPT中的
 *      索引信息生成为一个段，如图1所示，RAMPerThreadHardLimitMB影响的是一个DWPT。
 *
 *      如果所有DWPT（例如图1中的三个DWPT）中的DocumentIndexData的索引内存占用量超过RAMBufferSizeMB，那么就会触发自动flush，
 *      将DWPT中的索引信息生成为一个段，RAMPerThreadHardLimitMB影响的是所有的DWPT。
 * - MergedSegmentWarmer: 预热合并后的新段，它描述的是在执行段的合并期间，提前获得合并后生成的新段的信息，由于段的合并和文档的
 *      增删改是并发操作，所以使用该配置可以提高性能。默认null；
 * - UseCompoundFile: 当该值为true，那么通过flush、commit的操作生成索引使用的数据结构都是复合索引文件，即索引文件.cfs、.cfe。默认为true；
 *      注意的是执行段的合并后生成的新段对应的索引文件，即使通过上述方法另UseCompoundFile为true，但还是有可能生成非复合索引文件;
 * - CommitOnClose: 会影响IndexWriter.close()的执行逻辑，如果设置为true，那么会先应用（apply）所有的更改，即执行commit操作，
 *      否则上一次commit操作后的所有更改都不会保存，直接退出。默认为true；
 * - CheckPendingFlushUpdate：如果设置为true，那么当一个执行添加或更新文档操作的线程完成处理文档的工作后，会尝试去帮助待flush的DWPT。
 *
 * Holds all the configuration used by {@link IndexWriter} with few setters for
 * settings that can be changed on an {@link IndexWriter} instance "live".
 * 
 * @since 4.0
 */
public class LiveIndexWriterConfig {
  
  private final Analyzer analyzer;
  
  private volatile int maxBufferedDocs;
  private volatile double ramBufferSizeMB;
  private volatile IndexReaderWarmer mergedSegmentWarmer;

  // modified by IndexWriterConfig
  /** {@link IndexDeletionPolicy} controlling when commit
   *  points are deleted. */
  protected volatile IndexDeletionPolicy delPolicy;

  /** {@link IndexCommit} that {@link IndexWriter} is
   *  opened on. */
  protected volatile IndexCommit commit;

  /** {@link OpenMode} that {@link IndexWriter} is opened
   *  with. */
  protected volatile OpenMode openMode;

  /** Compatibility version to use for this index. */
  protected int createdVersionMajor = Version.LATEST.major;

  /** {@link Similarity} to use when encoding norms. */
  protected volatile Similarity similarity;

  /** {@link MergeScheduler} to use for running merges. */
  protected volatile MergeScheduler mergeScheduler;

  /** {@link IndexingChain} that determines how documents are
   *  indexed. */
  protected volatile IndexingChain indexingChain;

  /** {@link Codec} used to write new segments. */
  protected volatile Codec codec;

  /** {@link InfoStream} for debugging messages. */
  protected volatile InfoStream infoStream;

  /** {@link MergePolicy} for selecting merges. */
  protected volatile MergePolicy mergePolicy;

  /** {@code DocumentsWriterPerThreadPool} to control how
   *  threads are allocated to {@code DocumentsWriterPerThread}. */
  protected volatile DocumentsWriterPerThreadPool indexerThreadPool;

  /** True if readers should be pooled. */
  protected volatile boolean readerPooling;

  /** {@link FlushPolicy} to control when segments are
   *  flushed. */
  protected volatile FlushPolicy flushPolicy;

  /** Sets the hard upper bound on RAM usage for a single
   *  segment, after which the segment is forced to flush. */
  protected volatile int perThreadHardLimitMB;

  /** True if segment flushes should use compound file format */
  protected volatile boolean useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
  
  /** True if calls to {@link IndexWriter#close()} should first do a commit. */
  protected boolean commitOnClose = IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE;

  /** The sort order to use to write merged segments. */
  protected Sort indexSort = null;

  /** The field names involved in the index sort */
  protected Set<String> indexSortFields = Collections.emptySet();

  /** if an indexing thread should check for pending flushes on update in order to help out on a full flush*/
  protected volatile boolean checkPendingFlushOnUpdate = true;

  /** soft deletes field */
  protected String softDeletesField = null;

  /** the attributes for the NRT readers */
  protected Map<String, String> readerAttributes = Collections.emptyMap();


  // used by IndexWriterConfig
  LiveIndexWriterConfig(Analyzer analyzer) {
    this.analyzer = analyzer;
    ramBufferSizeMB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
    maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;
    mergedSegmentWarmer = null;
    delPolicy = new KeepOnlyLastCommitDeletionPolicy();
    commit = null;
    useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
    openMode = OpenMode.CREATE_OR_APPEND;
    similarity = IndexSearcher.getDefaultSimilarity();
    mergeScheduler = new ConcurrentMergeScheduler();
    indexingChain = DocumentsWriterPerThread.defaultIndexingChain;
    codec = Codec.getDefault();
    if (codec == null) {
      throw new NullPointerException();
    }
    infoStream = InfoStream.getDefault();
    mergePolicy = new TieredMergePolicy();
    flushPolicy = new FlushByRamOrCountsPolicy();
    readerPooling = IndexWriterConfig.DEFAULT_READER_POOLING;
    indexerThreadPool = new DocumentsWriterPerThreadPool();
    perThreadHardLimitMB = IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB;
  }
  
  /** Returns the default analyzer to use for indexing documents. */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Determines the amount of RAM that may be used for buffering added documents
   * and deletions before they are flushed to the Directory. Generally for
   * faster indexing performance it's best to flush by RAM usage instead of
   * document count and use as large a RAM buffer as you can.
   * <p>
   * When this is set, the writer will flush whenever buffered documents and
   * deletions use this much RAM. Pass in
   * {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent triggering a flush
   * due to RAM usage. Note that if flushing by document count is also enabled,
   * then the flush will be triggered by whichever comes first.
   * <p>
   * The maximum RAM limit is inherently determined by the JVMs available
   * memory. Yet, an {@link IndexWriter} session can consume a significantly
   * larger amount of memory than the given RAM limit since this limit is just
   * an indicator when to flush memory resident documents to the Directory.
   * Flushes are likely happen concurrently while other threads adding documents
   * to the writer. For application stability the available memory in the JVM
   * should be significantly larger than the RAM buffer used for indexing.
   * <p>
   * <b>NOTE</b>: the account of RAM usage for pending deletions is only
   * approximate. Specifically, if you delete by Query, Lucene currently has no
   * way to measure the RAM usage of individual Queries so the accounting will
   * under-estimate and you should compensate by either calling commit() or refresh()
   * periodically yourself.
   * <p>
   * <b>NOTE</b>: It's not guaranteed that all memory resident documents are
   * flushed once this limit is exceeded. Depending on the configured
   * {@link FlushPolicy} only a subset of the buffered documents are flushed and
   * therefore only parts of the RAM buffer is released.
   * <p>
   * 
   * The default value is {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB}.
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   * 
   * @throws IllegalArgumentException
   *           if ramBufferSize is enabled but non-positive, or it disables
   *           ramBufferSize when maxBufferedDocs is already disabled
   */
  public synchronized LiveIndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0) {
      throw new IllegalArgumentException("ramBufferSize should be > 0.0 MB when enabled");
    }
    if (ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
    this.ramBufferSizeMB = ramBufferSizeMB;
    return this;
  }

  /** Returns the value set by {@link #setRAMBufferSizeMB(double)} if enabled. */
  public double getRAMBufferSizeMB() {
    return ramBufferSizeMB;
  }
  
  /**
   * Determines the minimal number of documents required before the buffered
   * in-memory documents are flushed as a new Segment. Large values generally
   * give faster indexing.
   * 
   * <p>
   * When this is set, the writer will flush every maxBufferedDocs added
   * documents. Pass in {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent
   * triggering a flush due to number of buffered documents. Note that if
   * flushing by RAM usage is also enabled, then the flush will be triggered by
   * whichever comes first.
   * 
   * <p>
   * Disabled by default (writer flushes by RAM usage).
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see #setRAMBufferSizeMB(double)
   * @throws IllegalArgumentException
   *           if maxBufferedDocs is enabled but smaller than 2, or it disables
   *           maxBufferedDocs when ramBufferSize is already disabled
   */
  public synchronized LiveIndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    if (maxBufferedDocs != IndexWriterConfig.DISABLE_AUTO_FLUSH && maxBufferedDocs < 2) {
      throw new IllegalArgumentException("maxBufferedDocs must at least be 2 when enabled");
    }
    if (maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
    this.maxBufferedDocs = maxBufferedDocs;
    return this;
  }

  /**
   * Returns the number of buffered added documents that will trigger a flush if
   * enabled.
   *
   * @see #setMaxBufferedDocs(int)
   */
  public int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  /**
   * Expert: {@link MergePolicy} is invoked whenever there are changes to the
   * segments in the index. Its role is to select which merges to do, if any,
   * and return a {@link MergePolicy.MergeSpecification} describing the merges.
   * It also selects merges to do for forceMerge.
   * 
   * <p>
   * Takes effect on subsequent merge selections. Any merges in flight or any
   * merges already registered by the previous {@link MergePolicy} are not
   * affected.
   */
  public LiveIndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    if (mergePolicy == null) {
      throw new IllegalArgumentException("mergePolicy must not be null");
    }
    this.mergePolicy = mergePolicy;
    return this;
  }

  /**
   * Set the merged segment warmer. See {@link IndexReaderWarmer}.
   * 
   * <p>
   * Takes effect on the next merge.
   */
  public LiveIndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    this.mergedSegmentWarmer = mergeSegmentWarmer;
    return this;
  }

  /** Returns the current merged segment warmer. See {@link IndexReaderWarmer}. */
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return mergedSegmentWarmer;
  }
  
  /** Returns the {@link OpenMode} set by {@link IndexWriterConfig#setOpenMode(OpenMode)}. */
  public OpenMode getOpenMode() {
    return openMode;
  }

  /**
   * Return the compatibility version to use for this index.
   * @see IndexWriterConfig#setIndexCreatedVersionMajor
   */
  public int getIndexCreatedVersionMajor() {
    return createdVersionMajor;
  }

  /**
   * Returns the {@link IndexDeletionPolicy} specified in
   * {@link IndexWriterConfig#setIndexDeletionPolicy(IndexDeletionPolicy)} or
   * the default {@link KeepOnlyLastCommitDeletionPolicy}/
   */
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }
  
  /**
   * Returns the {@link IndexCommit} as specified in
   * {@link IndexWriterConfig#setIndexCommit(IndexCommit)} or the default,
   * {@code null} which specifies to open the latest index commit point.
   */
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: returns the {@link Similarity} implementation used by this
   * {@link IndexWriter}.
   */
  public Similarity getSimilarity() {
    return similarity;
  }
  
  /**
   * Returns the {@link MergeScheduler} that was set by
   * {@link IndexWriterConfig#setMergeScheduler(MergeScheduler)}.
   */
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }
  
  /** Returns the current {@link Codec}. */
  public Codec getCodec() {
    return codec;
  }

  /**
   * Returns the current MergePolicy in use by this writer.
   *
   * @see IndexWriterConfig#setMergePolicy(MergePolicy)
   */
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
  
  /**
   * Returns the configured {@link DocumentsWriterPerThreadPool} instance.
   * 
   * @see IndexWriterConfig#setIndexerThreadPool(DocumentsWriterPerThreadPool)
   * @return the configured {@link DocumentsWriterPerThreadPool} instance.
   */
  DocumentsWriterPerThreadPool getIndexerThreadPool() {
    return indexerThreadPool;
  }

  /**
   * Returns {@code true} if {@link IndexWriter} should pool readers even if
   * {@link DirectoryReader#open(IndexWriter)} has not been called.
   */
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /**
   * Returns the indexing chain.
   */
  IndexingChain getIndexingChain() {
    return indexingChain;
  }

  /**
   * Returns the max amount of memory each {@link DocumentsWriterPerThread} can
   * consume until forcefully flushed.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   */
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  
  /**
   * @see IndexWriterConfig#setFlushPolicy(FlushPolicy)
   */
  FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  /** Returns {@link InfoStream} used for debugging.
   *
   * @see IndexWriterConfig#setInfoStream(InfoStream)
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  /**
   * Sets if the {@link IndexWriter} should pack newly written segments in a
   * compound file. Default is <code>true</code>.
   * <p>
   * Use <code>false</code> for batch indexing with very large ram buffer
   * settings.
   * </p>
   * <p>
   * <b>Note: To control compound file usage during segment merges see
   * {@link MergePolicy#setNoCFSRatio(double)} and
   * {@link MergePolicy#setMaxCFSSegmentSizeMB(double)}. This setting only
   * applies to newly created segments.</b>
   * </p>
   */
  public LiveIndexWriterConfig setUseCompoundFile(boolean useCompoundFile) {
    this.useCompoundFile = useCompoundFile;
    return this;
  }
  
  /**
   * Returns <code>true</code> iff the {@link IndexWriter} packs
   * newly written segments in a compound file. Default is <code>true</code>.
   */
  public boolean getUseCompoundFile() {
    return useCompoundFile ;
  }
  
  /**
   * Returns <code>true</code> if {@link IndexWriter#close()} should first commit before closing.
   */
  public boolean getCommitOnClose() {
    return commitOnClose;
  }

  /**
   * Get the index-time {@link Sort} order, applied to all (flushed and merged) segments.
   */
  public Sort getIndexSort() {
    return indexSort;
  }

  /**
   * Returns the field names involved in the index sort
   */
  public Set<String> getIndexSortFields() {
    return indexSortFields;
  }

  /**
   * Expert: Returns if indexing threads check for pending flushes on update in order
   * to help our flushing indexing buffers to disk
   * @lucene.experimental
   */
  public boolean isCheckPendingFlushOnUpdate() {
    return checkPendingFlushOnUpdate;
  }

  /**
   * Expert: sets if indexing threads check for pending flushes on update in order
   * to help our flushing indexing buffers to disk. As a consequence, threads calling
   * {@link DirectoryReader#openIfChanged(DirectoryReader, IndexWriter)} or {@link IndexWriter#flush()} will
   * be the only thread writing segments to disk unless flushes are falling behind. If indexing is stalled
   * due to too many pending flushes indexing threads will help our writing pending segment flushes to disk.
   *
   * @lucene.experimental
   */
  public LiveIndexWriterConfig setCheckPendingFlushUpdate(boolean checkPendingFlushOnUpdate) {
    this.checkPendingFlushOnUpdate = checkPendingFlushOnUpdate;
    return this;
  }

  /**
   * Returns the soft deletes field or <code>null</code> if soft-deletes are disabled.
   * See {@link IndexWriterConfig#setSoftDeletesField(String)} for details.
   */
  public String getSoftDeletesField() {
    return softDeletesField;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("analyzer=").append(analyzer == null ? "null" : analyzer.getClass().getName()).append("\n");
    sb.append("ramBufferSizeMB=").append(getRAMBufferSizeMB()).append("\n");
    sb.append("maxBufferedDocs=").append(getMaxBufferedDocs()).append("\n");
    sb.append("mergedSegmentWarmer=").append(getMergedSegmentWarmer()).append("\n");
    sb.append("delPolicy=").append(getIndexDeletionPolicy().getClass().getName()).append("\n");
    IndexCommit commit = getIndexCommit();
    sb.append("commit=").append(commit == null ? "null" : commit).append("\n");
    sb.append("openMode=").append(getOpenMode()).append("\n");
    sb.append("similarity=").append(getSimilarity().getClass().getName()).append("\n");
    sb.append("mergeScheduler=").append(getMergeScheduler()).append("\n");
    sb.append("codec=").append(getCodec()).append("\n");
    sb.append("infoStream=").append(getInfoStream().getClass().getName()).append("\n");
    sb.append("mergePolicy=").append(getMergePolicy()).append("\n");
    sb.append("indexerThreadPool=").append(getIndexerThreadPool()).append("\n");
    sb.append("readerPooling=").append(getReaderPooling()).append("\n");
    sb.append("perThreadHardLimitMB=").append(getRAMPerThreadHardLimitMB()).append("\n");
    sb.append("useCompoundFile=").append(getUseCompoundFile()).append("\n");
    sb.append("commitOnClose=").append(getCommitOnClose()).append("\n");
    sb.append("indexSort=").append(getIndexSort()).append("\n");
    sb.append("checkPendingFlushOnUpdate=").append(isCheckPendingFlushOnUpdate()).append("\n");
    sb.append("softDeletesField=").append(getSoftDeletesField()).append("\n");
    sb.append("readerAttributes=").append(getReaderAttributes()).append("\n");
    return sb.toString();
  }

  /**
   * Returns the reader attributes passed to all published readers opened on or within the IndexWriter
   */
  public Map<String, String> getReaderAttributes() {
    return this.readerAttributes;
  }
}
