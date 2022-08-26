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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.WALInputStream;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX,
  HBaseInterfaceAudience.CONFIG})
public abstract class AbstractProtobufLogReader extends ReaderBase {
  private static final Log LOG = LogFactory.getLog(AbstractProtobufLogReader.class);

  protected static List<String> writerClsNames = new ArrayList<String>();
  /**
   * Configuration name of WAL Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, providers should log a warning. e.g. this is used with Protobuf reader/writer.
   */
  static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

  protected WALInputStream inputStream;
  protected Codec.Decoder cellDecoder;
  protected WALCellCodec.ByteStringUncompressor byteStringUncompressor;
  protected boolean hasCompression = false;
  protected boolean hasTagCompression = false;
  // walEditsStopOffset is the position of the last byte to read. After reading the last WALEdit
  // entry in the wal, the inputstream's position is equal to walEditsStopOffset.
  protected long walEditsStopOffset;
  protected boolean trailerPresent;
  protected WALProtos.WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;
  protected boolean isRecoveredEdits;
  // cell codec classname
  protected String codecClsName = null;

  public AbstractProtobufLogReader() {
    super();
  }


  public void init(Configuration conf, InputStream inputStream) throws IOException {
    this.conf = conf;
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    this.isRecoveredEdits = checkRecoveredEdits();

    String cellCodecClsName = initReader(inputStream);

    boolean compression = hasCompression();
    if (compression) {
      // If compression is enabled, new dictionaries are created here.
      try {
        if (compressionContext == null) {
          compressionContext = new CompressionContext(LRUDictionary.class,
            this.isRecoveredEdits, hasTagCompression());
        } else {
          compressionContext.clear();
        }
      } catch (Exception e) {
        throw new IOException("Failed to initialize CompressionContext", e);
      }
    }
    initAfterCompression(cellCodecClsName);
  }

  // context for WALHdr carrying information such as Cell Codec classname
  static class WALHdrContext {
    ProtobufLogReader.WALHdrResult result;
    String cellCodecClsName;

    WALHdrContext(ProtobufLogReader.WALHdrResult result, String cellCodecClsName) {
      this.result = result;
      this.cellCodecClsName = cellCodecClsName;
    }
    ProtobufLogReader.WALHdrResult getResult() {
      return result;
    }
    String getCellCodecClsName() {
      return cellCodecClsName;
    }
  }

  protected abstract boolean checkRecoveredEdits();

  @Override
  public void close() throws IOException {
    if (this.inputStream != null) {
      this.inputStream.close();
      this.inputStream = null;
    }
  }

  @Override
  protected boolean hasCompression() {
    return this.hasCompression;
  }

  @Override
  protected boolean hasTagCompression() {
    return this.hasTagCompression;
  }

  @Override
  protected void initAfterCompression(String cellCodecClsName) throws IOException {
    WALCellCodec codec = getCodec(this.conf, cellCodecClsName, this.compressionContext);
    this.cellDecoder = codec.getDecoder(this.inputStream);
    if (this.hasCompression) {
      this.byteStringUncompressor = codec.getByteStringUncompressor();
    }
  }

  protected WALCellCodec getCodec(Configuration conf, String cellCodecClsName,
    CompressionContext compressionContext) throws IOException {
    return WALCellCodec.create(conf, cellCodecClsName, compressionContext);
  }

  /*
   * Returns names of the accepted writer classes
   */
  public List<String> getWriterClsNames() {
    return writerClsNames;
  }
}
