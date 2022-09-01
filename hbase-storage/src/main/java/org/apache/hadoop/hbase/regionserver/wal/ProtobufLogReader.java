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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_COMPLETE_MAGIC;
import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_MAGIC;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader.Builder;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.WALInputStream;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.FileSystemBasedReader;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Protobuf based WAL has the following structure:
 * <p>
 * &lt;PB_WAL_MAGIC&gt;&lt;WALHeader&gt;&lt;WALEdits&gt;...&lt;WALEdits&gt;&lt;Trailer&gt;
 * &lt;TrailerSize&gt; &lt;PB_WAL_COMPLETE_MAGIC&gt;
 * </p>
 * The Reader reads meta information (WAL Compression state, WALTrailer, etc) in
 * ProtobufLogReader#initReader(FSDataInputStream). A WALTrailer is an extensible structure
 * which is appended at the end of the WAL. This is empty for now; it can contain some meta
 * information such as Region level stats, etc in future.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX,
  HBaseInterfaceAudience.CONFIG})
public class ProtobufLogReader extends AbstractProtobufLogReader implements FileSystemBasedReader {
  private static final Log LOG = LogFactory.getLog(ProtobufLogReader.class);

  protected FileSystem fs;
  protected Path path;

  static {
    writerClsNames.add(ProtobufLogWriter.class.getSimpleName());
  }

  @InterfaceAudience.Private
  public long trailerSize() {
    if (trailerPresent) {
      // sizeof PB_WAL_COMPLETE_MAGIC + sizof trailerSize + trailer
      final long calculatedSize = (long) PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT +
        trailer.getSerializedSize();
      final long expectedSize = logLength - walEditsStopOffset;
      if (expectedSize != calculatedSize) {
        LOG.warn("After parsing the trailer, we expect the total footer to be "+ expectedSize
          + " bytes, but we calculate it as being " + calculatedSize);
      }
      return expectedSize;
    } else {
      return -1L;
    }
  }

  enum WALHdrResult {
    EOF,                   // stream is at EOF when method starts
    SUCCESS,
    UNKNOWN_WRITER_CLS     // name of writer class isn't recognized
  }

  public ProtobufLogReader() {
    super();
  }

  @Override
  public long getPosition() throws IOException {
    return inputStream.getPos();
  }

  @Override
  public void reset() throws IOException {
    String clsName = initInternal(null, false);
    initAfterCompression(clsName); // We need a new decoder (at least).
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, FSDataInputStream stream)
      throws IOException {
    this.path = path;
    this.fs = fs;
    this.logLength = this.fs.getFileStatus(path).getLen();
    super.init(conf, stream);
  }

  @Override
  protected String initReader(InputStream stream) throws IOException {
    return initInternal((FSDataInputStream) stream, true);
  }

  @Override
  protected boolean checkRecoveredEdits() {
    return FSUtils.isRecoveredEdits(this.path);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (compressionContext != null && emptyCompressionContext) {
      while (next() != null) {
        if (getPosition() == pos) {
          emptyCompressionContext = false;
          break;
        }
      }
    }
    seekOnFs(pos);
  }
  
  /*
   * Returns the cell codec classname
   */
  public String getCodecClsName() {
    return codecClsName;
  }

  private String initInternal(FSDataInputStream stream, boolean isFirst)
      throws IOException {
    close();
    if (!isFirst) {
      // Re-compute the file length.
      this.logLength = fs.getFileStatus(path).getLen();
    }
    long expectedPos = PB_WAL_MAGIC.length;
    if (stream == null) {
      stream = fs.open(path);
      stream.seek(expectedPos);
    }
    if (stream.getPos() != expectedPos) {
      throw new IOException("The stream is at invalid position: " + stream.getPos());
    }
    // Initialize metadata or, when we reset, just skip the header.
    WALProtos.WALHeader.Builder builder = WALProtos.WALHeader.newBuilder();
    WALHdrContext hdrCtxt = readHeader(builder, stream);
    WALHdrResult walHdrRes = hdrCtxt.getResult();
    if (walHdrRes == WALHdrResult.EOF) {
      throw new EOFException("Couldn't read WAL PB header");
    }
    if (walHdrRes == WALHdrResult.UNKNOWN_WRITER_CLS) {
      throw new IOException("Got unknown writer class: " + builder.getWriterClsName());
    }
    if (isFirst) {
      WALProtos.WALHeader header = builder.build();
      this.hasCompression = header.hasHasCompression() && header.getHasCompression();
      this.hasTagCompression = header.hasHasTagCompression() && header.getHasTagCompression();
    }
    this.inputStream = new FSWALInputStream(stream);
    this.walEditsStopOffset = this.logLength;
    long currentPosition = stream.getPos();
    trailerPresent = setTrailerIfPresent();
    this.seekOnFs(currentPosition);
    if (LOG.isTraceEnabled()) {
      LOG.trace("After reading the trailer: walEditsStopOffset: " + this.walEditsStopOffset
        + ", logLength: " + this.logLength + ", " + "trailerPresent: "
        + (trailerPresent ? "true, size: " + trailer.getSerializedSize() : "false")
        + ", currentPosition: " + currentPosition);
    }
    
    codecClsName = hdrCtxt.getCellCodecClsName();
    
    return hdrCtxt.getCellCodecClsName();
  }

  @Override
  protected IOException extractHiddenEof(Exception ex) {
    // There are two problems we are dealing with here. Hadoop stream throws generic exception
    // for EOF, not EOFException; and scanner further hides it inside RuntimeException.
    IOException ioEx = null;
    if (ex instanceof EOFException) {
      return (EOFException)ex;
    } else if (ex instanceof IOException) {
      ioEx = (IOException)ex;
    } else if (ex instanceof RuntimeException
        && ex.getCause() != null && ex.getCause() instanceof IOException) {
      ioEx = (IOException)ex.getCause();
    }
    if (ioEx != null) {
      if (ioEx.getMessage().contains("EOF")) {
        return ioEx;
      }
      return null;
    }
    return null;
  }

  /**
   * Performs a filesystem-level seek to a certain position in an underlying file.
   */
  protected void seekOnFs(long pos) throws IOException {
    this.inputStream.seek(pos);
  }

  @Override
  protected String getLogName() {
    return this.path.toString();
  }

  static class FSWALInputStream extends WALInputStream {
    private final FSDataInputStream stream;
    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public FSWALInputStream(InputStream in) {
      super(in);
      stream = ((FSDataInputStream) in);
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      stream.seek(pos);
    }
  }
}
