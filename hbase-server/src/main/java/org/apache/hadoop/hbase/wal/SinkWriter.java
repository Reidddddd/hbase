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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class wraps the actual writer which writes data out and related statistics
 */
@InterfaceAudience.Private
public abstract class SinkWriter {
  /* Count of edits written to this path */
  long editsWritten = 0;
  /* Count of edits skipped to this path */
  long editsSkipped = 0;
  /* Number of nanos spent writing to this log */
  long nanosSpent = 0;

  void incrementEdits(int edits) {
    editsWritten += edits;
  }

  void incrementSkippedEdits(int skipped) {
    editsSkipped += skipped;
  }

  void incrementNanoTime(long nanos) {
    nanosSpent += nanos;
  }
}
