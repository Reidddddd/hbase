/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.wal;

import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LogReplayOutputSink extends AbstractLogReplayOutputSink {
  private final FileSystem walFS;
  private final AbstractWALSplitter walSplitter;

  public LogReplayOutputSink(PipelineController controller, EntryBuffers entryBuffers,
      int numWriters, Configuration conf, Set<TableName> disablingOrDisabledTables,
      Map<String, Long> lastFlushedSequenceIds, BaseCoordinatedStateManager csm,
      Map<String, Map<byte[], Long>> regionMaxSeqIdInStores, String failedServerName,
      FileSystem walFS, AbstractWALSplitter walSplitter) {
    super(controller, entryBuffers, numWriters, conf, disablingOrDisabledTables,
      lastFlushedSequenceIds, csm, regionMaxSeqIdInStores, failedServerName);
    this.walFS = walFS;
    this.walSplitter = walSplitter;
  }

  @Override
  protected OutputSink createLegacyOutputSink(PipelineController controller,
      EntryBuffers entryBuffers, int numWriters) {
    return new LogRecoveredEditsOutputSink(controller, entryBuffers, numWriters, walFS, conf,
      walSplitter, regionMaxSeqIdInStores);
  }
}
