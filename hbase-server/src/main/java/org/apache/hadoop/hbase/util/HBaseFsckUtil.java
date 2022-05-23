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

import static org.apache.hadoop.hbase.util.FSUtils.getFamilyDirs;
import static org.apache.hadoop.hbase.util.FSUtils.listStatusWithStatusFilter;
import com.google.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.CheckForNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class HBaseFsckUtil {
  private static final Log LOG = LogFactory.getLog(HBaseFsckUtil.class);

  // Set private constructor for util class.
  private HBaseFsckUtil() {}
  /**
   * Runs through the HBase rootdir and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(
      final FileSystem fs, final Path hbaseRootDir)
      throws IOException, InterruptedException {
    return getTableStoreFilePathMap(fs, hbaseRootDir, null, null, null);
  }

  /**
   * Runs through the HBase rootdir and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param sfFilter optional path filter to apply to store files
   * @param executor optional executor service to parallelize this operation
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(
      final FileSystem fs, final Path hbaseRootDir, PathFilter sfFilter,
      ExecutorService executor, ErrorReporter errors)
      throws IOException, InterruptedException {
    ConcurrentHashMap<String, Path> map = new ConcurrentHashMap<String, Path>(1024, 0.75f, 32);

    // if this method looks similar to 'getTableFragmentation' that is because
    // it was borrowed from it.

    // only include the directory paths to tables
    for (Path tableDir : FSUtils.getTableDirs(fs, hbaseRootDir)) {
      getTableStoreFilePathMap(map, fs, hbaseRootDir,
          FSUtils.getTableName(tableDir), sfFilter, executor, errors);
    }
    return map;
  }

  /**
   * Runs through the HBase rootdir/tablename and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param map map to add values.  If null, this method will create and populate one to return
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param tableName name of the table to scan.
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(Map<String, Path> map,
      final FileSystem fs, final Path hbaseRootDir, TableName tableName)
      throws IOException, InterruptedException {
    return getTableStoreFilePathMap(map, fs, hbaseRootDir, tableName, null, null, null);
  }

  /**
   * Runs through the HBase rootdir/tablename and creates a reverse lookup map for
   * table StoreFile names to the full Path.  Note that because this method can be called
   * on a 'live' HBase system that we will skip files that no longer exist by the time
   * we traverse them and similarly the user of the result needs to consider that some
   * entries in this map may not exist by the time this call completes.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param resultMap map to add values. If null, this method will create and populate one to return
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param tableName name of the table to scan.
   * @param sfFilter optional path filter to apply to store files
   * @param executor optional executor service to parallelize this operation
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(
      Map<String, Path> resultMap,
      final FileSystem fs, final Path hbaseRootDir, TableName tableName, final PathFilter sfFilter,
      ExecutorService executor, final ErrorReporter errors)
    throws IOException, InterruptedException {

    final Map<String, Path> finalResultMap =
        resultMap == null ? new ConcurrentHashMap<String, Path>(128, 0.75f, 32) : resultMap;

    // only include the directory paths to tables
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, tableName);
    // Inside a table, there are compaction.dir directories to skip.  Otherwise, all else
    // should be regions.
    final FSUtils.FamilyDirFilter familyFilter = new FSUtils.FamilyDirFilter(fs);
    final Vector<Exception> exceptions = new Vector<Exception>();

    try {
      List<FileStatus>
          regionDirs = listStatusWithStatusFilter(fs, tableDir, new FSUtils.RegionDirFilter(fs));
      if (regionDirs == null) {
        return finalResultMap;
      }

      final List<Future<?>> futures = new ArrayList<Future<?>>(regionDirs.size());

      for (FileStatus regionDir : regionDirs) {
        if (null != errors) {
          errors.progress();
        }
        final Path dd = regionDir.getPath();

        if (!exceptions.isEmpty()) {
          break;
        }

        Runnable getRegionStoreFileMapCall = new Runnable() {
          @Override
          public void run() {
            try {
              HashMap<String,Path> regionStoreFileMap = new HashMap<String, Path>();
              List<FileStatus> familyDirs = listStatusWithStatusFilter(fs, dd, familyFilter);
              if (familyDirs == null) {
                if (!fs.exists(dd)) {
                  LOG.warn("Skipping region because it no longer exists: " + dd);
                } else {
                  LOG.warn("Skipping region because it has no family dirs: " + dd);
                }
                return;
              }
              for (FileStatus familyDir : familyDirs) {
                if (null != errors) {
                  errors.progress();
                }
                Path family = familyDir.getPath();
                if (family.getName().equals(HConstants.RECOVERED_EDITS_DIR)) {
                  continue;
                }
                // now in family, iterate over the StoreFiles and
                // put in map
                FileStatus[] familyStatus = fs.listStatus(family);
                for (FileStatus sfStatus : familyStatus) {
                  if (null != errors) {
                    errors.progress();
                  }
                  Path sf = sfStatus.getPath();
                  if (sfFilter == null || sfFilter.accept(sf)) {
                    regionStoreFileMap.put(sf.getName(), sf);
                  }
                }
              }
              finalResultMap.putAll(regionStoreFileMap);
            } catch (Exception e) {
              LOG.error("Could not get region store file map for region: " + dd, e);
              exceptions.add(e);
            }
          }
        };

        // If executor is available, submit async tasks to exec concurrently, otherwise
        // just do serial sync execution
        if (executor != null) {
          Future<?> future = executor.submit(getRegionStoreFileMapCall);
          futures.add(future);
        } else {
          FutureTask<?> future = new FutureTask<Object>(getRegionStoreFileMapCall, null);
          future.run();
          futures.add(future);
        }
      }

      // Ensure all pending tasks are complete (or that we run into an exception)
      for (Future<?> f : futures) {
        if (!exceptions.isEmpty()) {
          break;
        }
        try {
          f.get();
        } catch (ExecutionException e) {
          LOG.error("Unexpected exec exception!  Should've been caught already.  (Bug?)", e);
          // Shouldn't happen, we already logged/caught any exceptions in the Runnable
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot execute getTableStoreFilePathMap for " + tableName, e);
      exceptions.add(e);
    } finally {
      if (!exceptions.isEmpty()) {
        // Just throw the first exception as an indication something bad happened
        // Don't need to propagate all the exceptions, we already logged them all anyway
        Throwables.propagateIfInstanceOf(exceptions.firstElement(), IOException.class);
        throw Throwables.propagate(exceptions.firstElement());
      }
    }

    return finalResultMap;
  }

  public static int getRegionReferenceFileCount(final FileSystem fs, final Path p) {
    int result = 0;
    try {
      for (Path familyDir:getFamilyDirs(fs, p)){
        result += getReferenceFilePaths(fs, familyDir).size();
      }
    } catch (IOException e) {
      LOG.warn("Error Counting reference files.", e);
    }
    return result;
  }

  public static List<Path> getReferenceFilePaths(final FileSystem fs, final Path familyDir)
      throws IOException {
    List<FileStatus> fds = listStatusWithStatusFilter(fs, familyDir, new ReferenceFileFilter(fs));
    if (fds == null) {
      return new ArrayList<Path>();
    }
    List<Path> referenceFiles = new ArrayList<Path>(fds.size());
    for (FileStatus fdfs: fds) {
      Path fdPath = fdfs.getPath();
      referenceFiles.add(fdPath);
    }
    return referenceFiles;
  }

  /**
   * Filter for HFileLinks (StoreFiles and HFiles not included).
   * the filter itself does not consider if a link is file or not.
   */
  public static class HFileLinkFilter implements PathFilter {

    @Override
    public boolean accept(Path p) {
      return HFileLink.isHFileLink(p);
    }
  }

  public static class ReferenceFileFilter extends AbstractFileStatusFilter {

    private final FileSystem fs;

    public ReferenceFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!StoreFileInfo.isReference(p)) {
        return false;
      }

      try {
        // only files can be references.
        return isFile(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
        return false;
      }
    }
  }

  /**
   * Filter for HFiles that excludes reference files.
   */
  public static class HFileFilter extends AbstractFileStatusFilter {
    final FileSystem fs;

    public HFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!StoreFileInfo.isHFile(p)) {
        return false;
      }

      try {
        return isFile(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
        return false;
      }
    }
  }
}
