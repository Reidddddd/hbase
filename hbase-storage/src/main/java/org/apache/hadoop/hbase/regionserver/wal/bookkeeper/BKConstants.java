/**
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
package org.apache.hadoop.hbase.regionserver.wal.bookkeeper;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class BKConstants {
  public static final String BK_CONFIG_FILE = "bkConfig.properties";
  public static final String BK_DIGEST_TYPE_KEY = "DigestType";
  public static final String BK_DIGEST_TYPE_DEFAULT = "MAC";
  public static final String BK_PASSWORD_KEY = "Password";
  public static final String BK_PASSWORD_DEFAULT = "hbase";
  public static final String BK_NUM_ENSEMBLE_KEY = "Ensembles";
  public static final int BK_NUM_ENSEMBLE_DEFAULT = 3;
  public static final String BK_NUM_QUORUM_KEY = "Quorums";
  public static final int BK_NUM_QUORUM_DEFAULT = 3;
  public static final String BK_NUM_ACK_KEY = "Acks";
  public static final int BK_NUM_ACK_DEFAULT = 1;

  public static final String LEDGER_META_ZK_QUORUMS = "hbase.ledger.meta.zk.quorums";
  public static final String DEFAULT_LEDGER_META_ZK_QUORUMS = "localhost:2181";
  public static final String LEDGER_ROOT_PATH = "hbase.ledger.root.path";
  public static final String DEFAULT_LEDGER_ROOT_PATH = "/hbase";

  public static final String LEDGER_COMPRESSED = "hbase.ledger.compressed";
  public static final Boolean DEFAULT_LEDGER_COMPRESSED = false;
  public static final String LEDGER_WRITER_SYNCED = "hbase.ledger.writer.synced";
  public static final boolean DEFAULT_LEDGER_WRITER_SYNCED = false;
  public static final byte[] EOL_BYTES = Bytes.toBytes("EOL");
  public static final byte[] SYNC_MARKER = Bytes.toBytes("SYNC");

  private BKConstants() {}
}
