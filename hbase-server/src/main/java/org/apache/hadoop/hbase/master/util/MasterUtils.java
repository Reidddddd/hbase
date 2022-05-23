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
package org.apache.hadoop.hbase.master.util;

import static org.apache.hadoop.hbase.util.FSUtils.getRootDir;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class MasterUtils {
  // Set private constructor for util class
  private MasterUtils() {}

  // TODO move this method OUT of FSUtils. No dependencies to HMaster
  /**
   * Returns the total overall fragmentation percentage. Includes hbase:meta and
   * -ROOT- as well.
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static int getTotalTableFragmentation(final HMaster master)
      throws IOException {
    Map<String, Integer> map = getTableFragmentation(master);
    return map != null && map.size() > 0 ? map.get("-TOTAL-") : -1;
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and hbase:meta too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   *
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
      final HMaster master)
      throws IOException {
    Path path = getRootDir(master.getConfiguration());
    // since HMaster.getFileSystem() is package private
    FileSystem fs = path.getFileSystem(master.getConfiguration());
    return getTableFragmentation(fs, path);
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and hbase:meta too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
      final FileSystem fs, final Path hbaseRootDir)
      throws IOException {
    Map<String, Integer> frags = new HashMap<String, Integer>();
    int cfCountTotal = 0;
    int cfFragTotal = 0;
    PathFilter regionFilter = new FSUtils.RegionDirFilter(fs);
    PathFilter familyFilter = new FSUtils.FamilyDirFilter(fs);
    List<Path> tableDirs = FSUtils.getTableDirs(fs, hbaseRootDir);
    for (Path d : tableDirs) {
      int cfCount = 0;
      int cfFrag = 0;
      FileStatus[] regionDirs = fs.listStatus(d, regionFilter);
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        // else its a region name, now look in region for families
        FileStatus[] familyDirs = fs.listStatus(dd, familyFilter);
        for (FileStatus familyDir : familyDirs) {
          cfCount++;
          cfCountTotal++;
          Path family = familyDir.getPath();
          // now in family make sure only one file
          FileStatus[] familyStatus = fs.listStatus(family);
          if (familyStatus.length > 1) {
            cfFrag++;
            cfFragTotal++;
          }
        }
      }
      // compute percentage per table and store in result list
      frags.put(FSUtils.getTableName(d).getNameAsString(),
          cfCount == 0? 0: Math.round((float) cfFrag / cfCount * 100));
    }
    // set overall percentage for all tables
    frags.put("-TOTAL-",
        cfCountTotal == 0? 0: Math.round((float) cfFragTotal / cfCountTotal * 100));
    return frags;
  }
}
