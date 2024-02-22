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

import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_COMPLETE_MAGIC;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.filesystem.ProtobufLogReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.WALInputStream;
import org.apache.hadoop.hbase.wal.Entry;
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
  protected long logLength;

  protected enum WALHdrResult {
    EOF,                   // stream is at EOF when method starts
    SUCCESS,
    UNKNOWN_WRITER_CLS     // name of writer class isn't recognized
  }

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
  protected static class WALHdrContext {
    WALHdrResult result;
    String cellCodecClsName;

    WALHdrContext(WALHdrResult result, String cellCodecClsName) {
      this.result = result;
      this.cellCodecClsName = cellCodecClsName;
    }
    public WALHdrResult getResult() {
      return result;
    }
    public String getCellCodecClsName() {
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

  protected WALHdrContext readHeader(WALProtos.WALHeader.Builder builder,  InputStream stream)
    throws IOException {
    boolean res = builder.mergeDelimitedFrom(stream);
    if (!res) {
      return new WALHdrContext(ProtobufLogReader.WALHdrResult.EOF, null);
    }
    if (builder.hasWriterClsName() &&
      !getWriterClsNames().contains(builder.getWriterClsName())) {
      return new WALHdrContext(ProtobufLogReader.WALHdrResult.UNKNOWN_WRITER_CLS, null);
    }
    String clsName = null;
    if (builder.hasCellCodecClsName()) {
      clsName = builder.getCellCodecClsName();
    }
    return new WALHdrContext(ProtobufLogReader.WALHdrResult.SUCCESS, clsName);
  }

  @Override
  protected void initAfterCompression() throws IOException {
    initAfterCompression(null);
  }

  /**
   * To check whether a trailer is present in a WAL, it seeks to position (fileLength -
   * PB_WAL_COMPLETE_MAGIC.size() - Bytes.SIZEOF_INT). It reads the int value to know the size of
   * the trailer, and checks whether the trailer is present at the end or not by comparing the last
   * PB_WAL_COMPLETE_MAGIC.size() bytes. In case trailer is not present, it returns false;
   * otherwise, sets the trailer and sets this.walEditsStopOffset variable up to the point just
   * before the trailer.
   * <ul>
   * The trailer is ignored in case:
   * <li>fileLength is 0 or not correct (when file is under recovery, etc).
   * <li>the trailer size is negative.
   * </ul>
   * <p>
   * In case the trailer size > this.trailerMaxSize, it is read after a WARN message.
   * @return true if a valid trailer is present
   */
  protected boolean setTrailerIfPresent() {
    try {
      long trailerSizeOffset = this.logLength - (PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT);
      if (trailerSizeOffset <= 0) {
        return false;// no trailer possible.
      }
      this.inputStream.seek(trailerSizeOffset);
      // read the int as trailer size.
      byte[] trailerSizeBytes = new byte[Bytes.SIZEOF_INT];
      this.inputStream.read(trailerSizeBytes);
      int trailerSize = Bytes.toInt(trailerSizeBytes);
      ByteBuffer buf = ByteBuffer.allocate(PB_WAL_COMPLETE_MAGIC.length);
      this.inputStream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      if (!Arrays.equals(buf.array(), PB_WAL_COMPLETE_MAGIC)) {
        LOG.trace("No trailer found.");
        return false;
      }
      if (trailerSize < 0) {
        LOG.warn("Invalid trailer Size " + trailerSize + ", ignoring the trailer");
        return false;
      } else if (trailerSize > this.trailerWarnSize) {
        // continue reading after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum configured size : "
          + trailerSize + " > " + this.trailerWarnSize);
      }
      // seek to the position where trailer starts.
      long positionOfTrailer = trailerSizeOffset - trailerSize;
      this.inputStream.seek(positionOfTrailer);
      // read the trailer.
      buf = ByteBuffer.allocate(trailerSize);// for trailer.
      this.inputStream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      trailer = WALProtos.WALTrailer.parseFrom(buf.array());
      this.walEditsStopOffset = positionOfTrailer;
      return true;
    } catch (IOException ioe) {
      LOG.warn("Got IOE while reading the trailer. Continuing as if no trailer is present.", ioe);
    }
    return false;
  }

  protected boolean readNext(Entry entry) throws IOException {
    while (true) {
      // OriginalPosition might be < 0 on local fs; if so, it is useless to us.
      long originalPosition = this.inputStream.getPos();
      if (trailerPresent && originalPosition > 0 && originalPosition == this.walEditsStopOffset) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Reached end of expected edits area at offset " + originalPosition);
        }
        return false;
      }
      WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
      long size = 0;
      boolean resetPosition = false;
      try {
        long available = -1;
        try {
          int firstByte = this.inputStream.read();
          if (firstByte == -1) {
            throw new EOFException("First byte is negative at offset " + originalPosition);
          }
          size = CodedInputStream.readRawVarint32(firstByte, this.inputStream);
          // available may be < 0 on local fs for instance.  If so, can't depend on it.
          available = this.inputStream.available();
          if (available > 0 && available < size) {
            throw new EOFException("Available stream not enough for edit, " +
              "inputStream.available()= " + this.inputStream.available() + ", " +
              "entry size= " + size + " at offset = " + this.inputStream.getPos());
          }
          ProtobufUtil.mergeFrom(builder, new LimitInputStream(this.inputStream, size),
            (int)size);
        } catch (InvalidProtocolBufferException ipbe) {
          resetPosition = true;
          throw (EOFException) new EOFException("Invalid PB, EOF? Ignoring; originalPosition=" +
            originalPosition + ", currentPosition=" + this.inputStream.getPos() +
            ", messageSize=" + size + ", currentAvailable=" + available).initCause(ipbe);
        }
        if (!builder.isInitialized()) {
          // TODO: not clear if we should try to recover from corrupt PB that looks semi-legit.
          //       If we can get the KV count, we could, theoretically, try to get next record.
          throw new EOFException("Partial PB while reading WAL, " +
            "probably an unexpected EOF, ignoring. current offset=" + this.inputStream.getPos());
        }
        WALProtos.WALKey walKey = builder.build();
        entry.getKey().readFieldsFromPb(walKey, this.byteStringUncompressor);
        if (!walKey.hasFollowingKvCount() || 0 == walKey.getFollowingKvCount()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset="
              + this.inputStream.getPos());
          }
          inputStream.seek(originalPosition);
          return false;
        }
        int expectedCells = walKey.getFollowingKvCount();
        long posBefore = this.inputStream.getPos();
        try {
          int actualCells = entry.getEdit().readFromCells(cellDecoder, expectedCells);
          if (expectedCells != actualCells) {
            resetPosition = true;
            throw new EOFException("Only read " + actualCells); // other info added in catch
          }
        } catch (Exception ex) {
          String posAfterStr = "<unknown>";
          try {
            posAfterStr = this.inputStream.getPos() + "";
          } catch (Throwable t) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Error getting pos for error message - ignoring", t);
            }
          }
          String message = " while reading " + expectedCells + " WAL KVs; started reading at "
            + posBefore + " and read up to " + posAfterStr;
          IOException realEofEx = extractHiddenEof(ex);
          throw (EOFException) new EOFException("EOF " + message).
            initCause(realEofEx != null ? realEofEx : ex);
        }
        if (trailerPresent && this.inputStream.getPos() > this.walEditsStopOffset) {
          LOG.error("Read WALTrailer while reading WALEdits. wal: " + this.getLogName()
            + ", inputStream.getPos(): " + this.inputStream.getPos() + ", walEditsStopOffset: "
            + this.walEditsStopOffset);
          throw new EOFException("Read WALTrailer while reading WALEdits");
        }
      } catch (EOFException eof) {
        // If originalPosition is < 0, it is rubbish and we cannot use it (probably local fs)
        if (originalPosition < 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Encountered a malformed edit, but can't seek back to last good position "
              + "because originalPosition is negative. last offset="
              + this.inputStream.getPos(), eof);
          }
          throw eof;
        }
        // If stuck at the same place and we got and exception, lets go back at the beginning.
        if (inputStream.getPos() == originalPosition && resetPosition) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Encountered a malformed edit, seeking to the beginning of the WAL since "
              + "current position and original position match at " + originalPosition);
          }
          inputStream.seek(0);
        } else {
          // Else restore our position to original location in hope that next time through we will
          // read successfully.
          if (LOG.isTraceEnabled()) {
            LOG.trace("Encountered a malformed edit, seeking back to last good position in file, "
              + "from " + inputStream.getPos()+" to " + originalPosition, eof);
          }
          inputStream.seek(originalPosition);
        }
        return false;
      }
      return true;
    }
  }

  protected abstract IOException extractHiddenEof(Exception ex);

  protected abstract String getLogName();
}
