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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

@Category({ MediumTests.class })
public class TestDynamicConfWithThroughputController {

  private static final Log LOG =
      LogFactory.getLog(TestDynamicConfWithThroughputController.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final double EPSILON = 1E-6;

  long throughputLimit = 1024L * 1024;

  @Test
  public void testThroughputTuning() throws Exception {

    File sourceConfFile = null;
    File bakConfFile = null;
    try {
      Configuration conf = TEST_UTIL.getConfiguration();
      conf.set("hbase.client.retries.number", "3");
      conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY,
          DefaultStoreEngine.class.getName());
      conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
      conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
      conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
      conf.set(
          CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
          PressureAwareCompactionThroughputController.class.getName());

      int tunePeriod = 5000;
      conf.setInt(
          PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
          tunePeriod);

      MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
      TEST_UTIL.startMiniCluster(1);
      HRegionServer regionServer =
          TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

      PressureAwareCompactionThroughputController throughputController =
          (PressureAwareCompactionThroughputController) regionServer.compactSplitThread
              .getCompactionThroughputController();
      throughputLimit =
          PressureAwareCompactionThroughputController.DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND;
      LOG.info("conf:" + conf.getResource("hbase-site.xml"));
      assertEquals(throughputLimit, throughputController.getMaxThroughput(),
          EPSILON);

      LOG.info("conf:" + conf.getResource("hbase-site.xml"));
      URL oldConfFile = conf.getResource("hbase-site.xml");

      sourceConfFile = new File(oldConfFile.getPath());
      File newConfFile = new File(oldConfFile.getPath() + "_new");
      bakConfFile = new File(oldConfFile.getPath() + "_bak_src");
      FileUtils.copyFile(sourceConfFile, newConfFile);
      FileUtils.copyFile(sourceConfFile, bakConfFile);

      addNewConf(newConfFile.getAbsolutePath());
      sourceConfFile.delete();
      FileUtils.moveFile(newConfFile, sourceConfFile);

      Thread.sleep(15 * 1000);

      LOG.info("updateConfiguration");
      Admin admin = TEST_UTIL.getConnection().getAdmin();
      LOG.info("regionServer.getServerName():" + regionServer.getServerName());
      admin.updateConfiguration(regionServer.getServerName());

      LOG.info(
          "assertEquals(throughputLimit*3, throughputController.getMaxThroughputUpperBound())");
      assertEquals(throughputLimit * 3,
          throughputController.getMaxThroughputUpperBound(), EPSILON);
      LOG.info(
          "assertEquals(throughputLimit*2, throughputController.getMaxThroughputLowerBound())");
      assertEquals(throughputLimit * 2,
          throughputController.getMaxThroughputLowerBound(), EPSILON);

      LOG.info("assertTrue OffPeakHour");
      for (int i = 0; i < 23; i++) {
        boolean isOffPeakHour =
            throughputController.getOffPeakHours().isOffPeakHour(i);
        LOG.info("assertTrue OffPeakHour:" + i);
        if (i < 6) {
          assertTrue("isOffPeakHour==false;check hour=" + i, isOffPeakHour);
        } else {
          assertTrue("isOffPeakHour==false;check hour=" + i, !isOffPeakHour);
        }
      }

      Thread.sleep(tunePeriod + 2000);
      LOG.info("assertTrue OffPeakHour");

      int hour = getHour(new Date());
      if (hour >= 0 && hour < 6) {
        assertEquals(throughputLimit * 4,
            throughputController.getMaxThroughput(), EPSILON);
      } else {
        assertEquals(throughputLimit * 2,
            throughputController.getMaxThroughput(), EPSILON);
      }

    } finally {
      sourceConfFile.delete();
      FileUtils.moveFile(bakConfFile, sourceConfFile);
    }

  }

  public void addNewConf(String newConfFile) {

    DocumentBuilderFactory bdg = DocumentBuilderFactory.newInstance();
    try {
      DocumentBuilder builder = bdg.newDocumentBuilder();

      Document dm = builder.parse(new FileInputStream(newConfFile));
      Element root = dm.getDocumentElement();

      Element propertyHigher = makeElement(dm,
          PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
          String.valueOf(throughputLimit * 3));
      root.appendChild(propertyHigher);

      Element propertLower = makeElement(dm,
          PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
          String.valueOf(throughputLimit * 2));
      root.appendChild(propertLower);

      Element propertyStartHour =
          makeElement(dm, "hbase.offpeak.start.hour", String.valueOf(0));
      root.appendChild(propertyStartHour);

      Element propertyEndHour =
          makeElement(dm, "hbase.offpeak.end.hour", String.valueOf(6));
      root.appendChild(propertyEndHour);

      Element propertyOffpeakLimit = makeElement(dm,
          PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK,
          String.valueOf(throughputLimit * 4));
      root.appendChild(propertyOffpeakLimit);

      Transformer tfd = TransformerFactory.newInstance().newTransformer();
      tfd.transform(new DOMSource(dm), new StreamResult(newConfFile));
    } catch (ParserConfigurationException e) {

    } catch (Exception e) {
      LOG.error("addNewConf error", e);
    }
  }

  private Element makeElement(Document dm, String key, String value) {
    Element property = dm.createElement("property");
    Element ename = dm.createElement("name");
    ename.setTextContent(key);
    Element evalue = dm.createElement("value");
    evalue.setTextContent(value);
    property.appendChild(ename);
    property.appendChild(evalue);

    return property;

  }

  public static int getHour(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.HOUR_OF_DAY);
  }

}
