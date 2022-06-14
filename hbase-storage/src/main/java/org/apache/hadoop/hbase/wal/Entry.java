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

import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility class that lets us keep track of the edit with it's key.
 */
@InterfaceAudience.Private
public class Entry {
  private final WALEdit edit;
  private final WALKey key;

  public Entry() {
    edit = new WALEdit();
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    key = new HLogKey();
  }

  /**
   * Constructor for both params
   *
   * @param edit log's edit
   * @param key log's key
   */
  public Entry(WALKey key, WALEdit edit) {
    super();
    this.key = key;
    this.edit = edit;
  }

  /**
   * Gets the edit
   *
   * @return edit
   */
  public WALEdit getEdit() {
    return edit;
  }

  /**
   * Gets the key
   *
   * @return key
   */
  public WALKey getKey() {
    return key;
  }

  /**
   * Set compression context for this entry.
   *
   * @param compressionContext
   *          Compression context
   */
  public void setCompressionContext(CompressionContext compressionContext) {
    edit.setCompressionContext(compressionContext);
    key.setCompressionContext(compressionContext);
  }

  @Override
  public String toString() {
    return this.key + "=" + this.edit;
  }

}
