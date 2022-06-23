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
import java.io.InterruptedIOException;
import java.util.Arrays;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;

public class WALUtils {
  private static final Log LOG = LogFactory.getLog(WALUtils.class);

  /**
   * Create a writer for the WAL.
   * should be package-private. public only for tests and
   * {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return A WAL writer.  Close when done with it.
   */
  public static Writer createWALWriter(final FileSystem fs, final Path path, Configuration conf)
    throws IOException {
    return createWriter(conf, fs, path, false);
  }

  /**
   * should be package-private, visible for recovery testing.
   * @return an overwritable writer for recovered edits. caller should close.
   */
  @VisibleForTesting
  public static Writer createRecoveredEditsWriter(final FileSystem fs, final Path path,
      Configuration conf) throws IOException {
    return createWriter(conf, fs, path, true);
  }

  /**
   * public because of FSHLog. Should be package-private
   */
  public static Writer createWriter(final Configuration conf, final FileSystem fs, final Path path,
    final boolean overwritable)
    throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends Writer> logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
      ProtobufLogWriter.class, Writer.class);
    Writer writer = null;
    try {
      writer = logWriterClass.getDeclaredConstructor().newInstance();
      FileSystem rootFs = FileSystem.get(path.toUri(), conf);
      if (writer instanceof FileSystemBasedWriter) {
        ((FileSystemBasedWriter) writer).init(rootFs, path, conf, overwritable);
      }
      return writer;
    } catch (Exception e) {
      LOG.debug("Error instantiating log writer.", e);
      if (writer != null) {
        try{
          writer.close();
        } catch(IOException ee){
          LOG.error("cannot close log writer", ee);
        }
      }
      throw new IOException("cannot get log writer", e);
    }
  }

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReader(final FileSystem fs, final Path path, Configuration conf)
    throws IOException {
    return createReader(fs, path, (CancelableProgressable)null, conf);
  }

  /**
   * Create a reader for the WAL. If you are reading from a file that's being written to and need
   * to reopen it multiple times, use {@link Reader#reset()} instead of this method
   * then just seek back to the last known good position.
   * @return A WAL reader.  Close when done with it.
   */
  public static Reader createReader(final FileSystem fs, final Path path,
      CancelableProgressable reporter, Configuration conf) throws IOException {
    return createReader(fs, path, reporter, true, conf);
  }

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  static Reader createReader(final FileSystem fs, final Path path,
    final Configuration conf, final CancelableProgressable reporter) throws IOException {
    return createReader(fs, path, reporter, conf);
  }

  /**
   * Create a reader for the given path, ignore custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * only public pending move of {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReaderIgnoreCustomClass(final FileSystem fs, final Path path,
    final Configuration conf) throws IOException {
    return createReader(fs, path, null, false, conf);
  }

  public static Reader createReader(final FileSystem fs, final Path path,
    CancelableProgressable reporter, boolean allowCustom, Configuration conf)
    throws IOException {
    int timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);

    Class<? extends Reader> lrClass = allowCustom ?
      conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
        Reader.class) : ProtobufLogReader.class;

    try {
      // A wal file could be under recovery, so it may take several
      // tries to get it open. Instead of claiming it is corrupted, retry
      // to open it up to 5 minutes by default.
      long startWaiting = EnvironmentEdgeManager.currentTime();
      long openTimeout = timeoutMillis + startWaiting;
      int nbAttempt = 0;
      FSDataInputStream stream = null;
      Reader reader = null;
      while (true) {
        try {
          if (lrClass != ProtobufLogReader.class) {
            // User is overriding the WAL reader, let them.
            reader = lrClass.getDeclaredConstructor().newInstance();
            if (reader instanceof FileSystemBasedReader) {
              ((FileSystemBasedReader) reader).init(fs, path, conf, null);
            }
            return reader;
          } else {
            stream = fs.open(path);
            // Note that zero-length file will fail to read PB magic, and attempt to create
            // a non-PB reader and fail the same way existing code expects it to. If we get
            // rid of the old reader entirely, we need to handle 0-size files differently from
            // merely non-PB files.
            byte[] magic = new byte[ProtobufLogReader.PB_WAL_MAGIC.length];
            boolean isPbWal = (stream.read(magic) == magic.length)
              && Arrays.equals(magic, ProtobufLogReader.PB_WAL_MAGIC);
            reader =
              isPbWal ? new ProtobufLogReader() : new SequenceFileLogReader();
            if (reader instanceof FileSystemBasedReader) {
              ((FileSystemBasedReader) reader).init(fs, path, conf, stream);
            }
            return reader;
          }
        } catch (IOException e) {
          if (stream != null) {
            try {
              stream.close();
            } catch (IOException exception) {
              LOG.warn("Could not close DefaultWALProvider.Reader" + exception.getMessage());
              LOG.debug("exception details", exception);
            }
          }
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException exception) {
              LOG.warn("Could not close FSDataInputStream" + exception.getMessage());
              LOG.debug("exception details", exception);
            }
          }
          String msg = e.getMessage();
          if (msg != null && (msg.contains("Cannot obtain block length")
            || msg.contains("Could not obtain the last block")
            || msg.matches("Blocklist for [^ ]* has changed.*"))) {
            if (++nbAttempt == 1) {
              LOG.warn("Lease should have recovered. This is not expected. Will retry", e);
            }
            if (reporter != null && !reporter.progress()) {
              throw new InterruptedIOException("Operation is cancelled");
            }
            if (nbAttempt > 2 && openTimeout < EnvironmentEdgeManager.currentTime()) {
              LOG.error("Can't open after " + nbAttempt + " attempts and "
                + (EnvironmentEdgeManager.currentTime() - startWaiting) + "ms " + " for " + path);
            } else {
              try {
                Thread.sleep(nbAttempt < 3 ? 500 : 1000);
                continue; // retry
              } catch (InterruptedException ie) {
                InterruptedIOException iioe = new InterruptedIOException();
                iioe.initCause(ie);
                throw iioe;
              }
            }
            throw new LeaseNotRecoveredException(e);
          } else {
            throw e;
          }
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }
}
