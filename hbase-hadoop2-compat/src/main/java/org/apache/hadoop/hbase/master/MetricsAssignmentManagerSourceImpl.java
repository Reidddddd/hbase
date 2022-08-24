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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

@InterfaceAudience.Private
public class MetricsAssignmentManagerSourceImpl
    extends BaseSourceImpl
    implements MetricsAssignmentManagerSource {

  private MutableGaugeLong ritGauge;
  private MutableGaugeLong ritCountOverThresholdGauge;
  private MutableGaugeLong ritOldestAgeGauge;
  private MetricHistogram ritDurationHisto;
  private MetricHistogram assignTimeHisto;
  private MetricHistogram bulkAssignTimeHisto;
  private MutableGaugeLong onlineRegionGauge;
  private MutableGaugeLong offlineRegionGauge;
  private MutableGaugeLong failedRegionGauge;
  private MutableGaugeLong splitRegionGauge;
  private MutableGaugeLong otherRegionGauge;

  public MetricsAssignmentManagerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsAssignmentManagerSourceImpl(String metricsName,
                                            String metricsDescription,
                                            String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  public void init() {
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, "", 0l);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(RIT_COUNT_OVER_THRESHOLD_NAME, "", 0l);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, "", 0l);
    assignTimeHisto = metricsRegistry.newTimeHistogram(ASSIGN_TIME_NAME);
    bulkAssignTimeHisto = metricsRegistry.newTimeHistogram(BULK_ASSIGN_TIME_NAME);
    ritDurationHisto = metricsRegistry.newTimeHistogram(RIT_DURATION_NAME);
    onlineRegionGauge = metricsRegistry.newGauge(ONLINE_REGION_COUNT_NAME, "", 0l);
    offlineRegionGauge = metricsRegistry.newGauge(OFFLINE_REGION_COUNT_NAME, "", 0l);
    failedRegionGauge = metricsRegistry.newGauge(FAILED_REGION_COUNT_NAME, "", 0l);
    splitRegionGauge = metricsRegistry.newGauge(SPLIT_REGION_COUNT_NAME, "", 0l);
    otherRegionGauge = metricsRegistry.newGauge(OTHER_REGION_COUNT_NAME, "", 0l);
  }

  @Override
  public void updateAssignmentTime(long time) {
    assignTimeHisto.add(time);
  }

  @Override
  public void updateBulkAssignTime(long time) {
    bulkAssignTimeHisto.add(time);
  }

  public void setRIT(int ritCount) {
    ritGauge.set(ritCount);
  }

  public void setRITCountOverThreshold(int ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }

  public void setRITOldestAge(long ritCount) {
    ritOldestAgeGauge.set(ritCount);
  }

  @Override
  public void updateRitDuration(long duration) {
    ritDurationHisto.add(duration);
  }

  @Override
  public void setOnlineRegionCount(long onlineRegionCount) {
    onlineRegionGauge.set(onlineRegionCount);
  }

  @Override
  public void setOfflineRegionCount(long offlineRegionCount) {
    offlineRegionGauge.set(offlineRegionCount);
  }

  @Override
  public void setFailedRegionCount(long failedRegionCount) {
    failedRegionGauge.set(failedRegionCount);
  }

  @Override
  public void setSplitRegionCount(long splitRegionCount) {
    splitRegionGauge.set(splitRegionCount);
  }

  @Override
  public void setOtherRegionCount(long otherRegionCount) {
    otherRegionGauge.set(otherRegionCount);
  }
}
