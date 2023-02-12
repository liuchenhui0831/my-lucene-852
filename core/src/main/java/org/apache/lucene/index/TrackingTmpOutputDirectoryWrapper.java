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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * 该类用来记录新创建临时索引文件，即带有.tmp后缀的文件。在两阶段生成索引文件之第一阶段中
 * 可以知道，IndexWriter在调用addDocument()的方法时，flush()或者commit()前，就会
 * 生成.fdx、.fdt以及.tvd、.tvx索引文件，而如果IndexWriter配置IndexSort，那么在上述期间内
 * 就只会生成临时的索引文件，TrackingTmpOutputDirectoryWrapper会记录这些临时索引文件；
 */
final class TrackingTmpOutputDirectoryWrapper extends FilterDirectory {
  private final Map<String,String> fileNames = new HashMap<>();

  TrackingTmpOutputDirectoryWrapper(Directory in) {
    super(in);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    IndexOutput output = super.createTempOutput(name, "", context);
    fileNames.put(name, output.getName());
    return output;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    // keep the original file name if no match, it might be a temp file already
    String tmpName = fileNames.getOrDefault(name, name);
    return super.openInput(tmpName, context);
  }

  public Map<String, String> getTemporaryFiles() {
    return fileNames;
  }
}
