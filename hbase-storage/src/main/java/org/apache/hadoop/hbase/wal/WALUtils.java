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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.api.DistributedLogManager;
import org.apache.distributedlog.shaded.api.LogWriter;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.exceptions.AlreadyClosedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.yetus.audience.InterfaceAudience;

public class WALUtils {
  private static final Log LOG = LogFactory.getLog(WALUtils.class);

  public static final String WAL_DIR_NAME_DELIMITER = ",";
  // should be package private; more visible for use in FSHLog
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** The hbase:meta region's WAL filename extension */
  public static final String DISTRIBUTED_LOG_NAMESPACE_DELIMITER = "/";
  public static final String DISTRIBUTED_LOG_DEFAULT_NAMESPACE = "default";
  public static final String DISTRIBUTED_LOG_ARCHIVE_PREFIX = "oldWALs";

  @VisibleForTesting
  public static final String META_WAL_PROVIDER_ID = ".meta";
  static final String DEFAULT_PROVIDER_ID = "default";

  // Implementation details that currently leak in tests or elsewhere follow
  /** File Extension used while splitting an WAL into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";

  // public for WALFactory until we move everything to o.a.h.h.wal
  @InterfaceAudience.Private
  public static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");
  // public for TestWALSplit
  @InterfaceAudience.Private
  public static final byte[] PB_WAL_COMPLETE_MAGIC = Bytes.toBytes("LAWP");
  /**
   * Configuration name of WAL Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, providers should log a warning. e.g. this is used with Protobuf reader/writer.
   */
  public static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  public static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

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
      } else if (writer instanceof ServiceBasedWriter) {
        ((ServiceBasedWriter) writer).init(conf, path.toString());
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
            if (reader instanceof ServiceBasedReader) {
              ((ServiceBasedReader) reader).init(conf, path.toString());
            }
            return reader;
          } else {
            stream = fs.open(path);
            // Note that zero-length file will fail to read PB magic, and attempt to create
            // a non-PB reader and fail the same way existing code expects it to. If we get
            // rid of the old reader entirely, we need to handle 0-size files differently from
            // merely non-PB files.
            byte[] magic = new byte[PB_WAL_MAGIC.length];
            boolean isPbWal = (stream.read(magic) == magic.length)
              && Arrays.equals(magic, PB_WAL_MAGIC);
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

  /**
   * It returns the file create timestamp from the file name.
   * For name format see {@link #validateWALFilename(String)}
   * public until remaining tests move to o.a.h.h.wal
   * @param wal must not be null
   * @return the file number that is part of the WAL file name
   */
  @VisibleForTesting
  public static long extractFileNumFromWAL(final WAL wal) {
    final Path walName = ((FSHLog)wal).getCurrentFileName();
    if (walName == null) {
      throw new IllegalArgumentException("The WAL path couldn't be null");
    }
    final String[] walPathStrs = walName.toString().split("\\" + WAL_FILE_NAME_DELIMITER);
    return Long.parseLong(walPathStrs[walPathStrs.length - (isMetaFile(walName) ? 2:1)]);
  }

  /**
   * Pattern used to validate a WAL file name
   * see {@link #validateWALFilename(String)} for description.
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*("+META_WAL_PROVIDER_ID+")*");

  /**
   * A WAL file name is of the format:
   * &lt;wal-name&gt;{@link #WAL_FILE_NAME_DELIMITER}&lt;file-creation-timestamp&gt;[.meta].
   *
   * provider-name is usually made up of a server-name and a provider-id
   *
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an WAL, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateWALFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  /**
   * Construct the directory name for all WALs on a given server.
   *
   * @param serverName
   *          Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name, e.g.
   *         <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is
   *         <code>1.example.org,60030,12345</code>
   */
  public static String getWALDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Pulls a ServerName out of a Path generated according to our layout rules.
   *
   * In the below layouts, this method ignores the format of the logfile component.
   *
   * Current format:
   *
   * [base directory for hbase]/hbase/.logs/ServerName/logfile
   *      or
   * [base directory for hbase]/hbase/.logs/ServerName-splitting/logfile
   *
   * Expected to work for individual log files and server-specific directories.
   *
   * @return null if it's not a log file. Returns the ServerName of the region
   *         server that created this log file otherwise.
   */
  public static ServerName getServerNameFromWALDirectoryName(Configuration conf, String path)
    throws IOException {
    if (path == null
      || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String walDir = FSUtils.getWALRootDir(conf).toString();

    final StringBuilder startPathSB = new StringBuilder(walDir);
    if (!walDir.endsWith("/")) {
      startPathSB.append('/');
    }
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/")) {
      startPathSB.append('/');
    }
    final String startPath = startPathSB.toString();

    String fullPath;
    try {
      fullPath = FileSystem.get(conf).makeQualified(new Path(path)).toString();
    } catch (IllegalArgumentException e) {
      LOG.info("Call to makeQualified failed on " + path + " " + e.getMessage());
      return null;
    }

    if (!fullPath.startsWith(startPath)) {
      return null;
    }

    final String serverNameAndFile = fullPath.substring(startPath.length());

    if (serverNameAndFile.indexOf('/') < "a,0,0".length()) {
      // Either it's a file (not a directory) or it's not a ServerName format
      return null;
    }

    Path p = new Path(path);
    return getServerNameFromWALDirectoryName(p);
  }

  /**
   * This function returns region server name from a log file name which is in one of the following
   * formats:
   * <ul>
   *   <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;-splitting/...</li>
   *   <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;/...</li>
   * </ul>
   * @return null if the passed in logFile isn't a valid WAL file path
   */
  public static ServerName getServerNameFromWALDirectoryName(Path logFile) {
    String logDirName = logFile.getParent().getName();
    // We were passed the directory and not a file in it.
    if (logDirName.equals(HConstants.HREGION_LOGDIR_NAME)) {
      logDirName = logFile.getName();
    }
    ServerName serverName = null;
    if (logDirName.endsWith(SPLITTING_EXT)) {
      logDirName = logDirName.substring(0, logDirName.length() - SPLITTING_EXT.length());
    }
    try {
      serverName = ServerName.parseServerName(logDirName);
    } catch (IllegalArgumentException ex) {
      serverName = null;
      LOG.warn("Cannot parse a server name from path=" + logFile + "; " + ex.getMessage());
    }
    if (serverName != null && serverName.getStartcode() < 0) {
      LOG.warn("Invalid log file path=" + logFile);
      serverName = null;
    }
    return serverName;
  }

  public static boolean isMetaFile(Path p) {
    return isMetaFile(p.getName());
  }

  public static boolean isMetaFile(String p) {
    if (p != null && p.endsWith(META_WAL_PROVIDER_ID)) {
      return true;
    }
    return false;
  }

  /**
   * Get prefix of the log from its name, assuming WAL name in format of
   * log_prefix.filenumber.log_suffix @see {@link FSHLog#getCurrentFileName()}
   * @param name Name of the WAL to parse
   * @return prefix of the log
   */
  public static String getWALPrefixFromWALName(String name) {
    int endIndex = name.replaceAll(META_WAL_PROVIDER_ID, "").lastIndexOf(".");
    return name.substring(0, endIndex);
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  public static String getWALArchivePathStr(String archiveRoot, String p) {
    String[] layers = p.split(DISTRIBUTED_LOG_NAMESPACE_DELIMITER);
    return String.join(DISTRIBUTED_LOG_NAMESPACE_DELIMITER, archiveRoot, layers[layers.length - 1]);
  }

  /**
   * Checks if the given distributed logName is the one with 'recovered.edits'.
   * @return True if we recovered edits
   */
  public static boolean isRecoveredEdits(String logName) {
    return logName.contains(HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Change a given String in the form "/aaa/bbb/ccc.xxx" to the form "aaa/bbb/ccc.xxx
   * This is used for convert {@link Path#toString()} to a legal log name for DistributedLog.
   */
  public static String pathToDistributedLogName(Path logPath) {
    String pathStr = logPath.toString();
    String[] nameArray = pathStr.split(DISTRIBUTED_LOG_NAMESPACE_DELIMITER);
    List<String> nonEmpty = new ArrayList<>();
    for (String part : nameArray) {
      if (part != null && part.length() > 0) {
        nonEmpty.add(part);
      }
    }
    String result = String.join(DISTRIBUTED_LOG_NAMESPACE_DELIMITER, nonEmpty);
    return result.length() == 0 ? DISTRIBUTED_LOG_NAMESPACE_DELIMITER : result;
  }

  public static String getFullPathStringForDistributedLog(String parent, String logName) {
    return String.join(DISTRIBUTED_LOG_NAMESPACE_DELIMITER, parent, logName);
  }

  public static List<String> listLogsUnderPath(String pathStr, Namespace namespace)
    throws IOException {
    Iterator<String> iterator = namespace.getLogs(pathStr);
    List<String> logs = new ArrayList<>();
    while (iterator.hasNext()) {
      logs.add(iterator.next());
    }
    return logs;
  }

  public static List<String> listLogsUnderPath(Path path, Namespace namespace) throws IOException {
    return listLogsUnderPath(pathToDistributedLogName(path), namespace);
  }

  public static void checkEndOfStream(Namespace namespace, String logName) {
    try {
      if (namespace.logExists(logName)) {
        DistributedLogManager dlm = namespace.openLog(logName);
        checkEndOfStream(dlm);
        dlm.close();
      }
    } catch (IOException ioe) {
      // If the communication with DistributedLog is broken, the IOE should be raised also
      // by other parts and be handled.
      // We do not throw or handle the exception here.
      LOG.warn(ioe);
    }
  }

  public static void checkEndOfStream(DistributedLogManager dlm) {
    try {
      if (!dlm.isEndOfStreamMarked()) {
        LogWriter writer = dlm.openLogWriter();
        writer.markEndOfStream();
        writer.close();
      }
    } catch (AlreadyClosedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Try to close an already closed stream, ignored. " + dlm);
      }
    } catch (IOException ioe) {
      // If the communication with DistributedLog is broken, the IOE should be raised also
      // by other parts and be handled.
      // We do not throw or handle the exception here.
      LOG.warn("Close stream met exception: \n", ioe);
    }
  }

  public static List<String> listLogs(Namespace namespace, Path root, PathFilter filter)
    throws IOException {
    Iterator<String> iterator = namespace.getLogs(pathToDistributedLogName(root));
    List<String> result = new ArrayList<>();
    while (iterator.hasNext()) {
      String logName = iterator.next();
      Path actualPath = new Path(root, logName);
      if (filter == null || filter.accept(actualPath)) {
        result.add(pathToDistributedLogName(actualPath));
      }
    }
    return result;
  }

  public static List<Path> listLogPaths(Namespace namespace, Path root, PathFilter filter)
    throws IOException {
    Iterator<String> iterator = namespace.getLogs(pathToDistributedLogName(root));
    List<Path> result = new ArrayList<>();
    while (iterator.hasNext()) {
      String logName = iterator.next();
      Path actualPath = new Path(root, logName);
      if (filter == null || filter.accept(actualPath)) {
        result.add(actualPath);
      }
    }
    return result;
  }

  public static String getSplittingName(String logNameWithParent) {
    Path logPath = new Path(logNameWithParent);
    if (logPath.getParent().getName().contains(SPLITTING_EXT)) {
      // It is already a splitting log.
      return logNameWithParent;
    } else {
      return pathToDistributedLogName(new Path(logPath.getParent().getName() + SPLITTING_EXT,
        logPath.getName()));
    }
  }

  public static String getArchivedLogName(String logNameWithParent) {
    Path logPath = new Path(logNameWithParent);
    return pathToDistributedLogName(new Path(DISTRIBUTED_LOG_ARCHIVE_PREFIX, logPath.getName()));
  }

  public static void renameDistributedLogsPath(Namespace namespace, Path origin, Path target)
    throws IOException {
    Iterator<String> iterator = namespace.getLogs(pathToDistributedLogName(origin));
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    while (iterator.hasNext()) {
      String log = iterator.next();
      checkEndOfStream(namespace, pathToDistributedLogName(new Path(origin, log)));
      futures.add(namespace.renameLog(
        pathToDistributedLogName(new Path(origin, log)),
        pathToDistributedLogName(new Path(target, log))
      ));
    }
    for (CompletableFuture<Void> future : futures) {
      try {
        future.join();
      } catch (Exception e) {
        LOG.warn("Failed to rename logs from root " + origin + " to " + target, e);
        throw new IOException(e);
      }
    }
    // After renaming finished, remove the origin path.
    namespace.deleteLog(pathToDistributedLogName(origin));
  }
}
