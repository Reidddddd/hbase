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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.apache.hadoop.hbase.replication.ReplicationStateZKBase.getWALServerNameAndGroupNameFromWALName;
import static org.apache.hadoop.hbase.replication.ReplicationStateZKBase.getWALTimestampFromWALName;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Continuously scan the wal file directory on HDFS to obtain newly rolled wal files
 * and add them to the queue. held by ReplicationIndependentSourceManager.
 */
@InterfaceAudience.Private
public class ReplicationIndependentConsumerQueueChecker extends Thread {

  private static final Log LOG =
    LogFactory.getLog(ReplicationIndependentConsumerQueueChecker.class);
  private final ReplicationQueues replicationQueues;
  private final Set<Path> latestPaths;
  private final List<ReplicationSourceInterface> sources;
  private final Map<String, Map<String, SortedSet<String>>> walsById;

  private volatile boolean running = true;

  private CompletableFuture<Boolean> stopFuture = new CompletableFuture<>();

  public ReplicationIndependentConsumerQueueChecker(
    ReplicationQueues replicationQueues, Set<Path> latestPaths,
    List<ReplicationSourceInterface> sources,
    Map<String, Map<String, SortedSet<String>>> walsById) {
    this.replicationQueues = replicationQueues;
    this.latestPaths = latestPaths;
    this.sources = sources;
    this.walsById = walsById;
  }

  @Override
  public void run() {
    while (running) {
      try {
        // Get wals from hdfs path.
        List<Path> allLogsInPath = replicationQueues.getLogsInPath();
        Map<String, SortedSet<Path>> groupLogsMap =
          allLogsInPath.stream().collect(Collectors.groupingBy(
            path -> getWALServerNameAndGroupNameFromWALName(path.getName()),
            Collectors.toCollection(TreeSet::new)));

        // The core of this code is to scan the wal dir, filter out new files that need to be
        // replicated, and then add them to queue. We use latestPaths to record the latest wal
        // file, so we only need to distinguish between two scenarios:
        // 1. The latestPaths is empty when running for the first time. We need to compare wal
        //    with the currently consumed wal znode on zookeeper;
        // 2. Normally, it only needs to be compared with the latestPaths in the memory.
        synchronized (latestPaths) {
          // 1. First scenario, running for the first time.
          if (latestPaths.size() == 0) {
            // Get current consuming wals from zookeeper.
            Map<String, Map<String, String>> peer2CurrentConsumingLogsMap = new HashMap<>();
            sources.forEach(source -> {
              String peerId = source.getPeerClusterZnode();
              try {
                List<String> currentConsumingLogsInQueue =
                  replicationQueues.getCurrentConsumingLogsInQueue(peerId);
                Map<String, String> currentConsumingLogsMap =
                  currentConsumingLogsInQueue.stream().collect(Collectors.groupingBy(
                    name -> getWALServerNameAndGroupNameFromWALName(name),
                    Collectors.collectingAndThen(
                      Collectors.maxBy((name1, name2) ->
                        getWALTimestampFromWALName(name1).compareTo(name2)),
                      optional -> optional.get())));
                peer2CurrentConsumingLogsMap.put(peerId, currentConsumingLogsMap);
              } catch (KeeperException e) {
                throw new RuntimeException(e);
              }
            });

            // Find out wals not replicated and enqueue, for each group and each source.
            groupLogsMap.forEach((serverNameAndGroupName, logsSet) -> {
              latestPaths.add(logsSet.last());
              sources.forEach(source -> {
                String peerId = source.getPeerClusterZnode();
                Map<String, String> currentConsumingLogsMap =
                  peer2CurrentConsumingLogsMap.get(peerId);
                String currentConsumeLog = currentConsumingLogsMap.get(serverNameAndGroupName);
                logsSet.forEach(path -> {
                  if (getWALTimestampFromWALName(path.getName())
                    .compareTo(getWALTimestampFromWALName(currentConsumeLog)) >= 0) {
                    LOG.trace("Checker enqueue log: " + path.getName() +
                      ", current log: " + currentConsumeLog);
                    enqueueLog(serverNameAndGroupName, source, peerId, path);
                  }else {
                    LOG.info("Checker not enqueue log: " + path.getName() +
                      ", current log: " + currentConsumeLog);
                  }
                });
              });
            });
          } else {// 2. Normally scenario.
            // Find out wals not replicated and enqueue, for each group and each source.
            groupLogsMap.forEach((serverNameAndGroupName, logsSet) -> logsSet.forEach(path -> {
              Map<String, TreeSet<Path>> latestPathGroupByServerNameAndGroupName =
                latestPaths.stream().collect(Collectors.groupingBy(
                  p -> getWALServerNameAndGroupNameFromWALName(p.getName()),
                  Collectors.toCollection(TreeSet::new)));

              if (latestPathGroupByServerNameAndGroupName.containsKey(
                getWALServerNameAndGroupNameFromWALName(path.getName()))) {
                Path oldPath = latestPathGroupByServerNameAndGroupName.get(
                    getWALServerNameAndGroupNameFromWALName(path.getName()))
                  .first();
                if (getWALTimestampFromWALName(path.getName()).compareTo(
                  getWALTimestampFromWALName(oldPath.getName())) > 0) {
                  LOG.debug("Checker enqueue " + path.getName() + " because: " +
                    getWALTimestampFromWALName(path.getName()) + " > " + getWALTimestampFromWALName(
                    oldPath.getName()));
                  latestPaths.remove(oldPath);
                  latestPaths.add(path);

                  sources.forEach(source -> {
                    String peerId = source.getPeerClusterZnode();
                    enqueueLog(serverNameAndGroupName, source, peerId, path);
                  });
                }
              } else {// The new log belongs to a new group, enqueue it.
                latestPaths.add(path);
                sources.forEach(source -> {
                  String peerId = source.getPeerClusterZnode();
                  enqueueLog(serverNameAndGroupName, source, peerId, path);
                });
              }
            }));
          }
        }
        Thread.sleep(10 * 1000);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOG.info("Interrupt queue checker, running = " + running);
      } catch (Exception e) {
        if (running == false) {
          LOG.info("Stop queue checker, running = " + running);
          stopFuture.complete(true);
        }
      }
    }
    stopFuture.complete(true);
  }

  private void enqueueLog(String prefix, ReplicationSourceInterface source, String peerId,
                          Path path) {
    source.enqueueLog(path);
    synchronized (walsById) {
      Map<String, SortedSet<String>> walsByPrefix = walsById.get(peerId);
      if (!walsByPrefix.containsKey(prefix)) {
        SortedSet<String> wals = new TreeSet<>();
        walsByPrefix.put(prefix, wals);
      }
      if (!walsByPrefix.get(prefix).contains(path.getName())) {
        walsByPrefix.get(prefix).add(path.getName());
      }
    }
  }

  public CompletableFuture<Boolean> terminate() {
    running = false;
    this.interrupt();
    return stopFuture;
  }
}
