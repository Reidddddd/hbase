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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Contains some methods to control WAL-entries producer / consumer interactions
 */
@InterfaceAudience.Private
public class PipelineController {
  // If an exception is thrown by one of the other threads, it will be
  // stored here.
  AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

  // Wait/notify for when data has been produced by the writer thread,
  // consumed by the reader thread, or an exception occurred
  final Object dataAvailable = new Object();

  void writerThreadError(Throwable t) {
    thrown.compareAndSet(null, t);
  }

  /**
   * Check for errors in the writer threads. If any is found, rethrow it.
   */
  void checkForErrors() throws IOException {
    Throwable thrown = this.thrown.get();
    if (thrown == null) {
      return;
    }
    if (thrown instanceof IOException) {
      throw new IOException(thrown);
    } else {
      throw new RuntimeException(thrown);
    }
  }
}
