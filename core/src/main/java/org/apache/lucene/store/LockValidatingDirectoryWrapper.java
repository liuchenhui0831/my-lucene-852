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
package org.apache.lucene.store;


import java.io.IOException;
import java.util.Collection;

/** 
 * This class makes a best-effort check that a provided {@link Lock}
 * is valid before any destructive filesystem operation.
 *
 * 该类使得在执行创建、删除、重命名、同步(持久化索引文件至磁盘)这样的具有破坏性的操作前
 * 都会先检查索引文件锁的状态是否有效的，比如说如果用户手动的把write.lock文件删除，那么
 * 会导致索引文件锁失效。IndexWriter中使用了该类来维护索引文件；
 */
public final class LockValidatingDirectoryWrapper extends FilterDirectory {
  private final Lock writeLock;

  public LockValidatingDirectoryWrapper(Directory in, Lock writeLock) {
    super(in);
    this.writeLock = writeLock;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    writeLock.ensureValid();
    in.deleteFile(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    writeLock.ensureValid();
    return in.createOutput(name, context);
  }

  @Override
  public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
    writeLock.ensureValid();
    in.copyFrom(from, src, dest, context);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    writeLock.ensureValid();
    in.rename(source, dest);
  }

  @Override
  public void syncMetaData() throws IOException {
    writeLock.ensureValid();
    in.syncMetaData();
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    writeLock.ensureValid();
    in.sync(names);
  }
}
