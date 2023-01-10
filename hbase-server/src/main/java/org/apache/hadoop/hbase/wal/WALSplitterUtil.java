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

import static org.apache.hadoop.hbase.HConstants.RECOVERED_EDITS_DIR;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class WALSplitterUtil {
  private static final Log LOG = LogFactory.getLog(WALSplitterUtil.class);

  private static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  private static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  private static final String SEQUENCE_ID_FILE_SUFFIX = ".seqid";
  private static final String OLD_SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  private static final int SEQUENCE_ID_FILE_SUFFIX_LENGTH = SEQUENCE_ID_FILE_SUFFIX.length();

  private WALSplitterUtil() {

  }

  /**
   * Get the completed recovered edits file path, renaming it to be by last edit
   * in the file from its first edit. Then we could use the name to skip
   * recovered edits when doing {@link HRegion#replayRecoveredEditsIfAny}.
   * @return dstPath take file's last edit log seq num as the name
   */
  static Path getCompletedRecoveredEditsFilePath(Path srcPath, long maximumEditLogSeqNum) {
    String fileName = formatRecoveredEditsFileName(maximumEditLogSeqNum);
    return new Path(srcPath.getParent(), fileName);
  }

  @VisibleForTesting
  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in
   * <code>logEntry</code> named for the sequenceid in the passed
   * <code>logEntry</code>: e.g. /hbase/some_table/2323432434/recovered.edits/2332.
   * This method also ensures existence of RECOVERED_EDITS_DIR under the region
   * creating it if necessary.
   * @param fileNameBeingSplit the file being split currently. Used to generate tmp file name.
   * @param tmpDirName of the directory used to sideline old recovered edits file
   * @return Path to file into which to dump split log edits.
   */
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static Path getRegionSplitEditsPath(final Entry logEntry, String fileNameBeingSplit,
    String tmpDirName, Configuration conf) throws IOException {
    FileSystem walFS = FSUtils.getWALFileSystem(conf);
    TableName tableName = logEntry.getKey().getTablename();
    String encodedRegionName = Bytes.toString(logEntry.getKey().getEncodedRegionName());
    Path regionDir = FSUtils.getWALRegionDir(conf, tableName, encodedRegionName);
    Path dir = getRegionDirRecoveredEditsDir(regionDir);

    if (walFS.exists(dir) && walFS.isFile(dir)) {
      Path tmp = new Path(tmpDirName);
      if (!walFS.exists(tmp)) {
        walFS.mkdirs(tmp);
      }
      tmp = new Path(tmp, RECOVERED_EDITS_DIR + "_" + encodedRegionName);
      LOG.warn("Found existing old file: " + dir + ". It could be some "
        + "leftover of an old installation. It should be a folder instead. "
        + "So moving it to " + tmp);
      if (!walFS.rename(dir, tmp)) {
        LOG.warn("Failed to sideline old file " + dir);
      }
    }

    if (!walFS.exists(dir) && !walFS.mkdirs(dir)) {
      LOG.warn("mkdir failed on " + dir);
    } else {
      String storagePolicy =
        conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
      FSUtils.setStoragePolicy(walFS, dir, storagePolicy);
    }
    // Append fileBeingSplit to prevent name conflict since we may have duplicate wal entries now.
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName =
      WALSplitterUtil.formatRecoveredEditsFileName(logEntry.getKey().getLogSeqNum());
    fileName = getTmpRecoveredEditsFileName(fileName + "-" + fileNameBeingSplit);
    return new Path(dir, fileName);
  }

  static Path getRegionSplitEditsPath4DistributedLog(final Entry logEntry,
      String fileNameBeingSplit) {
    TableName tableName = logEntry.getKey().getTablename();
    String encodedRegionName = Bytes.toString(logEntry.getKey().getEncodedRegionName());

    Path regionPath = new Path(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
    regionPath = new Path(regionPath, new Path(encodedRegionName, RECOVERED_EDITS_DIR));

    String logEntity = new Path(fileNameBeingSplit).getName();
    String logName =
      WALSplitterUtil.formatRecoveredEditsFileName(logEntry.getKey().getLogSeqNum());
    logName = getTmpRecoveredEditsFileName(logName + "-" + logEntity);
    return new Path(regionPath, logName);
  }

  /**
   * @param regionDir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regionDir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regionDir) {
    return new Path(regionDir, RECOVERED_EDITS_DIR);
  }

  private static String getTmpRecoveredEditsFileName(String fileName) {
    return fileName + RECOVERED_LOG_TMPFILE_SUFFIX;
  }

  /**
   * Returns sorted set of edit files made by splitter, excluding files
   * with '.temp' suffix.
   *
   * @param walFS FileSystem to use for reading Recovered edits files
   * @param regionDir Directory where Recovered edits should reside
   * @return Files in passed <code>regionDir</code> as a sorted set.
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem walFS,
    final Path regionDir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regionDir);
    if (!walFS.exists(editsdir)) {
      return filesSorted;
    }
    FileStatus[] files = FSUtils.listStatus(walFS, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = walFS.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splitwal thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
          // Skip SeqId Files
          if (isSequenceIdFile(p)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null) {
      return filesSorted;
    }
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  public static NavigableSet<Path> getSplitEditLogsSorted(Namespace namespace,
      final Path regionDir) throws IOException {
    NavigableSet<Path> logsSorted = new TreeSet<>();
    Path editsDir = getRegionDirRecoveredEditsDir(regionDir);

    List<String> regionLogs = WALUtils.listLogsUnderPath(editsDir, namespace);
    for (String logName : regionLogs) {
      Matcher m = EDITFILES_NAME_PATTERN.matcher(logName);
      boolean result = m.matches();
      if (logName.endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
        result = false;
      }
      if (isSequenceIdFile(new Path(logName))) {
        result = false;
      }
      if (result) {
        LOG.info("Find log to replay: " + logName);
        logsSorted.add(new Path(editsDir, logName));
      }
    }
    return logsSorted;
  }

  /**
   * Returns sorted set of edit logs made by splitter, excluding log name
   * with '.temp' suffix.
   *
   * @param walNamespace Namespace to use for reading Recovered edits.
   * @param regionPath Log path where Recovered edits should reside
   * @return Files in passed <code>regionDir</code> as a sorted set.
   */
  public static NavigableSet<Path> getSplitEditFilesSorted4DistributedLog(
      final Namespace walNamespace, final Path regionPath) throws IOException {
    NavigableSet<Path> logsSorted = new TreeSet<Path>();
    Path editsPath = getRegionDirRecoveredEditsDir(regionPath);
    String editsPathStr = WALUtils.pathToDistributedLogName(editsPath);
    if (!walNamespace.logExists(editsPathStr)) {
      return logsSorted;
    }

    Iterator<String> iterator = walNamespace.getLogs(editsPathStr);
    while (iterator.hasNext()) {
      String logName = iterator.next();
      Matcher m = EDITFILES_NAME_PATTERN.matcher(logName);
      boolean result = m.matches();
      if (logName.endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
        result = false;
      }
      // Skip SeqId Files
      if (isSequenceIdFile(logName)) {
        result = false;
      }
      if (result) {
        logsSorted.add(new Path(editsPath, logName));
      }
    }
    return logsSorted;
  }

  /**
   * Move aside a bad edits file.
   *
   * @param walFS FileSystem to use for WAL operations
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   */
  public static Path moveAsideBadEditsFile(final FileSystem walFS, final Path edits)
    throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
      + System.currentTimeMillis());
    if (!walFS.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * Move aside a bad edits log.
   *
   * @param walNamespace {@link Namespace} to use for WAL operations
   * @param edits Edits log to move aside.
   * @return The name of the moved aside file.
   */
  public static Path moveAsideBadEditsLog(final Namespace walNamespace, final Path edits)
    throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
      + System.currentTimeMillis());
    String logStr = WALUtils.pathToDistributedLogName(edits);
    try {
      WALUtils.checkEndOfStream(walNamespace, logStr);
      walNamespace.renameLog(logStr, WALUtils.pathToDistributedLogName(moveAsideName)).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
    return moveAsideName;
  }

  /**
   * Is the given file a region open sequence id file.
   */
  @VisibleForTesting
  public static boolean isSequenceIdFile(final Path file) {
    return isSequenceIdFile(file.getName());
  }

  public static boolean isSequenceIdFile(final String file) {
    return file.endsWith(SEQUENCE_ID_FILE_SUFFIX)
      || file.endsWith(OLD_SEQUENCE_ID_FILE_SUFFIX);
  }

  /**
   * Create a file with name as region open sequence id
   * @param walFS FileSystem to write Sequence file to
   * @param regionDir WALRegionDir used to determine where to write edits files
   * @return long new sequence Id value
   */
  public static long writeRegionSequenceIdFile(final FileSystem walFS, final Path regionDir,
    long newSeqId, long saftyBumper) throws IOException {

    Path editsdir = getRegionDirRecoveredEditsDir(regionDir);
    long maxSeqId = 0;
    FileStatus[] files = null;
    if (walFS.exists(editsdir)) {
      files = FSUtils.listStatus(walFS, editsdir, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return isSequenceIdFile(p);
        }
      });
      if (files != null) {
        for (FileStatus status : files) {
          String fileName = status.getPath().getName();
          try {
            Long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
              - SEQUENCE_ID_FILE_SUFFIX_LENGTH));
            maxSeqId = Math.max(tmpSeqId, maxSeqId);
          } catch (NumberFormatException ex) {
            LOG.warn("Invalid SeqId File Name=" + fileName);
          }
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId

    // write a new seqId file
    Path newSeqIdFile = new Path(editsdir, newSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newSeqId != maxSeqId) {
      try {
        if (!walFS.createNewFile(newSeqIdFile) && !walFS.exists(newSeqIdFile)) {
          throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrote region seqId=" + newSeqIdFile + " to file, newSeqId=" + newSeqId
            + ", maxSeqId=" + maxSeqId);
        }
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    if (files != null) {
      for (FileStatus status : files) {
        if (newSeqIdFile.equals(status.getPath())) {
          continue;
        }
        walFS.delete(status.getPath(), false);
      }
    }
    return newSeqId;
  }

  public static long writeRegionSequenceIdLog(final Namespace namespace, final Path regionDir,
    long newSeqId, long saftyBumper) throws IOException {
    Path editsDir = getRegionDirRecoveredEditsDir(regionDir);
    long maxSeqId = 0;

    List<Path> logs = WALUtils.listLogPaths(namespace, editsDir, WALSplitterUtil::isSequenceIdFile);

    if (!logs.isEmpty()) {
      for (Path log : logs) {
        String fileName = log.getName();
        try {
          long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
            - SEQUENCE_ID_FILE_SUFFIX_LENGTH));
          maxSeqId = Math.max(tmpSeqId, maxSeqId);
        } catch (NumberFormatException ex) {
          LOG.warn("Invalid SeqId File Name=" + fileName);
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId

    // write a new seqId file
    Path newSeqIdFile = new Path(editsDir, newSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newSeqId != maxSeqId) {
      try {
        namespace.createLog(WALUtils.pathToDistributedLogName(newSeqIdFile));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrote region seqId=" + newSeqIdFile + " to file, newSeqId=" + newSeqId
            + ", maxSeqId=" + maxSeqId);
        }
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    if (logs.size() != 0) {
      for (Path log : logs) {
        if (newSeqIdFile.equals(log)) {
          continue;
        }
        namespace.deleteLog(WALUtils.pathToDistributedLogName(log));
      }
    }
    return newSeqId;
  }

  static Writer createWriter(Path logfile, FileSystem walFS, Configuration conf)
    throws IOException {
    return WALUtils.createRecoveredEditsWriter(walFS, logfile, conf);
  }

  static Reader getReader(Path curLogFile, CancelableProgressable reporter, FileSystem walFS,
      Configuration conf) throws IOException {
    return WALUtils.createReader(walFS, curLogFile, reporter, conf);
  }
}
