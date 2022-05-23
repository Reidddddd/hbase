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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.mapreduce.MapreduceTestingShim;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;

public class HBaseStorageTestUtility extends HBaseCommonTestingUtility {
  /**
   * System property key to get test directory value.
   * Name is as it is because mini dfs has hard-codings to put test data here.
   * It should NOT be used directly in HBase, as it's a property used in
   *  mini dfs.
   *  @deprecated can be used only with mini dfs
   */
  @Deprecated
  private static final String TEST_DIRECTORY_KEY = "test.build.data";
  /** Directory (a subdirectory of dataTestDir) used by the dfs cluster if any */
  private File clusterTestDir = null;

  private MiniDFSCluster dfsCluster = null;
  /** Directory on test filesystem where we put the data for this instance of
   * HBaseTestingUtility*/
  private Path dataTestDirOnTestFS = null;

  public static final String USE_LOCAL_FILESYSTEM = "hbase.test.local.fileSystem";

  /**
   * Start a minidfscluster.
   * @param servers How many DNs to start.
   * @see #shutdownMiniDFSCluster()
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null);
  }

  /**
   * Start a minidfscluster.
   * This is useful if you want to run datanode on distinct hosts for things
   * like HDFS block location verification.
   * If you start MiniDFSCluster without host names, all instances of the
   * datanodes will have the same host name.
   * @param hosts hostnames DNs to run on.
   * @see #shutdownMiniDFSCluster()
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(final String[] hosts)
      throws Exception {
    if (hosts != null && hosts.length != 0) {
      return startMiniDFSCluster(hosts.length, hosts);
    } else {
      return startMiniDFSCluster(1, null);
    }
  }

  /**
   * Creates an hbase rootdir in user home directory.  Also creates hbase
   * version file.  Normally you won't make use of this method.  Root hbasedir
   * is created for you as part of mini cluster startup.  You'd only use this
   * method if you were doing manual operation.
   * @param create This flag decides whether to get a new
   *               root or data directory path or not, if it has been fetched already.
   *               Note : Directory will be made irrespective of whether path has been fetched or
   *               not. If directory already exists, it will be overwritten
   * @return Fully qualified path to hbase root dir
   */
  public Path createRootDir(boolean create) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path hbaseRootdir = getDefaultRootDirPath(create);
    FSUtils.setRootDir(this.conf, hbaseRootdir);
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  /**
   * Same as {@link HBaseStorageTestUtility#createRootDir(boolean create)}
   * except that <code>create</code> flag is false.
   * @return Fully qualified path to hbase root dir
   */
  public Path createRootDir() throws IOException {
    return createRootDir(false);
  }

  /**
   * Creates a hbase walDir in the user's home directory.
   * Normally you won't make use of this method. Root hbaseWALDir
   * is created for you as part of mini cluster startup. You'd only use this
   * method if you were doing manual operation.
   *
   * @return Fully qualified path to hbase WAL root dir
   */
  public Path createWALRootDir() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path walDir = getNewDataTestDirOnTestFS();
    FSUtils.setWALRootDir(this.conf, walDir);
    fs.mkdirs(walDir);
    return walDir;
  }

  /**
   * Returns the path to the default root dir the minicluster uses. If <code>create</code>
   * is true, a new root directory path is fetched irrespective of whether it has been fetched
   * before or not. If false, previous path is used.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   */
  public Path getDefaultRootDirPath(boolean create) throws IOException {
    if (!create) {
      return getDataTestDirOnTestFS();
    } else {
      return getNewDataTestDirOnTestFS();
    }
  }

  public MiniDFSCluster startMiniDFSClusterForTestWAL(int namenodePort) throws IOException {
    createDirsAndSetProperties();
    dfsCluster = new MiniDFSCluster(namenodePort, conf, 5, false, true, true, null,
      null, null, null);
    return dfsCluster;
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param servers How many DNs to start.
   * @param hosts hostnames DNs to run on.
   * @see #shutdownMiniDFSCluster()
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers, final String[] hosts)
      throws Exception {
    createDirsAndSetProperties();
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);

    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).
        setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).
        setLevel(org.apache.log4j.Level.ERROR);


    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
        true, null, null, hosts, null);

    // Set this just-started cluster as our filesystem.
    setFs();

    // Wait for the cluster to be totally up
    this.dfsCluster.waitClusterUp();

    //reset the test directory for test file system
    dataTestDirOnTestFS = null;

    return this.dfsCluster;
  }

  /** This is used before starting HDFS and map-reduce mini-clusters */
  private void createDirsAndSetProperties() throws IOException {
    setupClusterTestDir();
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    createDirAndSetProperty("cache_data", "test.cache.data");
    createDirAndSetProperty("hadoop_tmp", "hadoop.tmp.dir");
    createDirAndSetProperty("mapred_local", "mapreduce.cluster.local.dir");
    createDirAndSetProperty("mapred_temp", "mapreduce.cluster.temp.dir");
    enableShortCircuit();

    Path root = getDataTestDirOnTestFS("hadoop");
    conf.set(MapreduceTestingShim.getMROutputDirProp(),
        new Path(root, "mapred-output-dir").toString());
    conf.set("mapreduce.jobtracker.system.dir", new Path(root, "mapred-system-dir").toString());
    conf.set("mapreduce.jobtracker.staging.root.dir",
        new Path(root, "mapreduce-jobtracker-staging-root-dir").toString());
    conf.set("mapreduce.job.working.dir", new Path(root, "mapred-working-dir").toString());
  }

  /**
   * Creates a directory for the DFS cluster, under the test data
   */
  private void setupClusterTestDir() {
    if (clusterTestDir != null) {
      return;
    }

    // Using randomUUID ensures that multiple clusters can be launched by
    //  a same test, if it stops & starts them
    Path testDir = getDataTestDir("dfscluster_" + UUID.randomUUID().toString());
    clusterTestDir = new File(testDir.toString()).getAbsoluteFile();
    // Have it cleaned up on exit
    boolean b = deleteOnExit();
    if (b) {
      clusterTestDir.deleteOnExit();
    }
    conf.set(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    LOG.info("Created new mini-cluster data directory: " + clusterTestDir + ", deleteOnExit=" + b);
  }

  private void setFs() throws IOException {
    if(this.dfsCluster == null){
      LOG.info("Skipping setting fs because dfsCluster is null");
      return;
    }
    FileSystem fs = this.dfsCluster.getFileSystem();
    FSUtils.setFsDefault(this.conf, new Path(fs.getUri()));
    if (this.conf.getBoolean(USE_LOCAL_FILESYSTEM, false)) {
      FSUtils.setFsDefault(this.conf, new Path("file:///"));
    }
  }

  private String createDirAndSetProperty(final String relPath, String property) {
    String path = getDataTestDir(relPath).toString();
    System.setProperty(property, path);
    conf.set(property, path);
    new File(path).mkdirs();
    LOG.info("Setting " + property + " to " + path + " in system properties and HBase conf");
    return path;
  }

  /** Enable the short circuit read, unless configured differently.
   * Set both HBase and HDFS settings, including skipping the hdfs checksum checks.
   */
  private void enableShortCircuit() {
    if (isReadShortCircuitOn()) {
      String curUser = System.getProperty("user.name");
      LOG.info("read short circuit is ON for user " + curUser);
      // read short circuit, for hdfs
      conf.set("dfs.block.local-path-access.user", curUser);
      // read short circuit, for hbase
      conf.setBoolean("dfs.client.read.shortcircuit", true);
      // Skip checking checksum, for the hdfs client and the datanode
      conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", true);
    } else {
      LOG.info("read short circuit is OFF");
    }
  }

  /**
   * Sets up a path in test filesystem to be used by tests.
   * Creates a new directory if not already setup.
   */
  private void setupDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS != null) {
      LOG.warn("Data test on test fs dir already setup in "
          + dataTestDirOnTestFS.toString());
      return;
    }
    dataTestDirOnTestFS = getNewDataTestDirOnTestFS();
  }

  /**
   * Sets up a new path in test filesystem to be used by tests.
   */
  private Path getNewDataTestDirOnTestFS() throws IOException {
    //The file system can be either local, mini dfs, or if the configuration
    //is supplied externally, it can be an external cluster FS. If it is a local
    //file system, the tests should use getBaseTestDir, otherwise, we can use
    //the working directory, and create a unique sub dir there
    FileSystem fs = getTestFileSystem();
    Path newDataTestDir = null;
    String randomStr = UUID.randomUUID().toString();
    if (fs.getUri().getScheme().equals(FileSystem.getLocal(conf).getUri().getScheme())) {
      newDataTestDir = new Path(getDataTestDir(), randomStr);
      File dataTestDir = new File(newDataTestDir.toString());
      if (deleteOnExit()) {
        dataTestDir.deleteOnExit();
      }
    } else {
      Path base = getBaseTestDirOnTestFS();
      newDataTestDir = new Path(base, randomStr);
      if (deleteOnExit()) {
        fs.deleteOnExit(newDataTestDir);
      }
    }
    return newDataTestDir;
  }

  /**
   * @return Where to write test data on the test filesystem; Returns working directory
   *         for the test filesystem by default
   * @see #setupDataTestDirOnTestFS()
   * @see #getTestFileSystem()
   */
  private Path getBaseTestDirOnTestFS() throws IOException {
    FileSystem fs = getTestFileSystem();
    return new Path(fs.getWorkingDirectory(), "test-data");
  }

  public FileSystem getTestFileSystem() throws IOException {
    return HFileSystem.get(conf);
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()}
   * to write temporary test data. Call this method after setting up the mini dfs cluster
   * if the test relies on it.
   * @return a unique path in the test filesystem
   * @param subdirName name of the subdir to create under the base test dir
   */
  public Path getDataTestDirOnTestFS(final String subdirName) throws IOException {
    return new Path(getDataTestDirOnTestFS(), subdirName);
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()}
   * to write temporary test data. Call this method after setting up the mini dfs cluster
   * if the test relies on it.
   * @return a unique path in the test filesystem
   */
  public Path getDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS == null) {
      setupDataTestDirOnTestFS();
    }

    return dataTestDirOnTestFS;
  }

  /**
   *  Get the HBase setting for dfs.client.read.shortcircuit from the conf or a system property.
   *  This allows to specify this parameter on the command line.
   *   If not set, default is true.
   */
  public boolean isReadShortCircuitOn(){
    final String propName = "hbase.tests.use.shortcircuit.reads";
    String readOnProp = System.getProperty(propName);
    if (readOnProp != null){
      return  Boolean.parseBoolean(readOnProp);
    } else {
      return conf.getBoolean(propName, false);
    }
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  /**
   * Shuts down instance created by call to {@link #startMiniDFSCluster(int)}
   * or does nothing.
   */
  public void shutdownMiniDFSCluster() throws IOException {
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
      dfsCluster = null;
      dataTestDirOnTestFS = null;
      FSUtils.setFsDefault(this.conf, new Path("file:///"));
    }
  }
}
