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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.HConstants.HREGION_LOGDIR_NAME;
import static org.apache.hadoop.hbase.HConstants.HREGION_OLDLOGDIR_NAME;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_CONFIG_FILE;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.DEFAULT_LEDGER_ROOT_PATH;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.LEDGER_ROOT_PATH;
import com.google.protobuf.ByteString;
import dlshade.org.apache.bookkeeper.client.BookKeeper;
import dlshade.org.apache.bookkeeper.client.api.BKException;
import dlshade.org.apache.bookkeeper.conf.ClientConfiguration;
import java.io.EOFException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Util class for ledger ops.
 */
@InterfaceAudience.Private
public final class LedgerUtil {
  public static final byte[] EMPTY_BYTES = new byte[0];

  private LedgerUtil() {

  }

  public static BookKeeper getBKClient(Configuration conf)
      throws IOException, ConfigurationException {
    return getBKClient(getBKClientConf(conf));
  }

  public static BookKeeper getBKClient(ClientConfiguration clientConfig) throws IOException {
    try {
      return new BookKeeper(clientConfig);
    } catch (BKException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  // This method will create and load a new object from local file each time.
  // The bkConfig.properties file should be located under hbase conf dir.
  public static ClientConfiguration getBKClientConf(Configuration conf)
      throws ConfigurationException {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    URL filePath = conf.getResource(BK_CONFIG_FILE);
    clientConfiguration.loadConf(filePath);
    clientConfiguration.setAllowShadedLedgerManagerFactoryClass(true);
    clientConfiguration.setShadedLedgerManagerFactoryClassPrefix("dlshade.");
    clientConfiguration.setNumChannelsPerBookie(3);
    clientConfiguration.setNettyMaxFrameSizeBytes(10 * 1024 * 1024); // 10MB
    return clientConfiguration;
  }

  public static String getLedgerRootPath(Configuration conf) {
    return conf.get(LEDGER_ROOT_PATH, DEFAULT_LEDGER_ROOT_PATH);
  }

  public static String getLedgerArchivePath(String rootPath) {
    return ZKUtil.joinZNode(rootPath, HREGION_OLDLOGDIR_NAME);
  }

  public static String getLedgerArchivePath(Configuration conf) {
    return getLedgerArchivePath(getLedgerRootPath(conf));
  }

  public static String getLedgerLogPath(String rootPath, String serverName) {
    return ZKUtil.joinZNode(getLedgerLogPath(rootPath), serverName);
  }

  public static String getLedgerLogPath(String rootPath) {
    return ZKUtil.joinZNode(rootPath, HREGION_LOGDIR_NAME);
  }

  public static WALProtos.LedgerEntry toLedgerEntryWithoutCompression(Entry entry)
      throws IOException {
    entry.setCompressionContext(null);
    WALProtos.WALKey walKey = entry.getKey().getBuilder(null)
      .setFollowingKvCount(entry.getEdit().size()).build();
    WALProtos.WALEdit.Builder editBuilder = WALProtos.WALEdit.newBuilder();

    List<ByteString> cells = entry.getEdit().getCells().stream().map(cell ->
      ByteString.copyFrom(KeyValueUtil.copyToNewByteArray(cell))).collect(Collectors.toList());
    editBuilder.addAllCells(cells);

    WALProtos.WALEdit walEdit = editBuilder.build();

    WALProtos.LedgerEntry.Builder entryBuilder = WALProtos.LedgerEntry.newBuilder();
    entryBuilder.setWalKey(walKey);
    entryBuilder.setWalEdit(walEdit);

    return entryBuilder.build();
  }

  /**
   * A util function to parse bytes to WAL entry.
   * @param data Bytes read from bk.
   * @param entry Reused object.
   * @return true if there are cells to carry on. False if there is no cells serialized.
   */
  public static boolean parseToEntry(byte[] data, Entry entry) throws IOException {
    WALProtos.LedgerEntry ledgerEntry = WALProtos.LedgerEntry.parseFrom(data);
    WALProtos.WALKey walKey = ledgerEntry.getWalKey();

    if (!walKey.hasFollowingKvCount() || walKey.getFollowingKvCount() == 0) {
      return false;
    }
    entry.getKey().readFieldsFromPb(walKey, null);

    int expectedCells = walKey.getFollowingKvCount();
    ArrayList<Cell> cells = new ArrayList<>(expectedCells);
    ledgerEntry.getWalEdit().getCellsList().forEach(
      oneCell -> cells.add(new KeyValue(oneCell.toByteArray())));

    int actualCells = cells.size();
    if (expectedCells != actualCells) {
      throw new EOFException("Wrong cells count, expected: " + expectedCells + " but got: "
        + actualCells);
    }

    entry.getEdit().setCells(cells);
    return true;
  }
}
