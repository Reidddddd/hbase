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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogReader;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.DistributedLogWALProvider;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestDistributedLogSplitManager extends TestSplitLogManager {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogSplitManager.class);

  private URI uri;
  private Namespace walNamespace;

  @BeforeClass
  public static void startup() throws Exception {
    setupCluster(numBookies);
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    conf.setClass("hbase.split.log.manager.class", DistributedLogSplitManager.class,
      SplitLogManager.class);
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);

    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class,
      Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class,
      Reader.class);
    conf.setClass("hbase.wal.provider", DistributedLogWALProvider.class, WALProvider.class);
    conf.setClass("hbase.wal.meta_provider", DistributedLogWALProvider.class, WALProvider.class);

    walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
  }

  @Override
  @Test(timeout=180000)
  public void testEmptyLogDir() throws Exception {
    LOG.info("testEmptyLogDir");
    slm = getSplitLogManager(conf);

    Path emptyLogDirPath = new Path("testEmptyLogDir", UUID.randomUUID().toString());
    walNamespace.createLog(WALUtils.pathToDistributedLogName(emptyLogDirPath));
    slm.splitLogDistributed(emptyLogDirPath);
    assertFalse(walNamespace.logExists(WALUtils.pathToDistributedLogName(emptyLogDirPath)));
  }

  @Override
  @Test (timeout = 60000)
  public void testLogFilesAreArchived() throws Exception {
    LOG.info("testLogFilesAreArchived");
    final SplitLogManager slm = getSplitLogManager(conf);

    Path dir = new Path("testLogFilesAreArchived");

    Path logDirPath = new Path(dir, UUID.randomUUID().toString());
    String logDirPathStr = WALUtils.pathToDistributedLogName(logDirPath);

    // create an empty log file
    String logFile = ServerName.valueOf("foo", 1, 1).toString();
    walNamespace.createLog(WALUtils.pathToDistributedLogName(new Path(logDirPath, logFile)));

    // spin up a thread mocking split done.
    new Thread() {
      @Override
      public void run() {
        boolean done = false;
        while (!done) {
          for (Map.Entry<String, SplitLogManager.Task> entry : slm.getTasks().entrySet()) {
            final ServerName worker1 = ServerName.valueOf("worker1,1,1");
            SplitLogTask slt = new SplitLogTask.Done(worker1,
              ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_SPLITTING);
            boolean encounteredZKException = false;
            try {
              ZKUtil.setData(zkw, entry.getKey(), slt.toByteArray());
            } catch (KeeperException e) {
              LOG.warn(e);
              encounteredZKException = true;
            }
            if (!encounteredZKException) {
              done = true;
            }
          }
        }
      };
    }.start();

    slm.splitLogDistributed(logDirPath);

    assertFalse(walNamespace.logExists(logDirPathStr));
  }
}
