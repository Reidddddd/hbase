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

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DummyWALObserver implements WALObserver {
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {

  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {

  }

  @Override
  public boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
    HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    return false;
  }

  @Override
  public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
    HLogKey logKey, WALEdit logEdit) throws IOException {
    return false;
  }

  @Override
  public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
    HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {

  }

  @Override
  public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
    HLogKey logKey, WALEdit logEdit) throws IOException {

  }

  @Override
  public void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx, Path oldPath,
    Path newPath) throws IOException {

  }

  @Override
  public void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx, Path oldPath,
    Path newPath) throws IOException {

  }
}
