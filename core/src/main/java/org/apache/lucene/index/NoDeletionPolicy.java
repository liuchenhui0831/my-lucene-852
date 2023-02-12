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


import java.util.List;

/**
 * 不会删除提交
 * 优点在于，配合segments_N文件和commitUserData可以将索引信息恢复到任意一个提交状态；
 * 缺点在于索引目录需要保留大量的索引文件
 *
 * An {@link IndexDeletionPolicy} which keeps all index commits around, never
 * deleting them. This class is a singleton and can be accessed by referencing
 * {@link #INSTANCE}.
 */
public final class NoDeletionPolicy extends IndexDeletionPolicy {

  /** The single instance of this class. */
  public static final IndexDeletionPolicy INSTANCE = new NoDeletionPolicy();
  
  private NoDeletionPolicy() {
    // keep private to avoid instantiation
  }
  
  @Override
  public void onCommit(List<? extends IndexCommit> commits) {}

  @Override
  public void onInit(List<? extends IndexCommit> commits) {}
}
