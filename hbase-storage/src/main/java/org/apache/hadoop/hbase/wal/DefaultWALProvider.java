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

import static org.apache.hadoop.hbase.wal.WALUtils.META_WAL_PROVIDER_ID;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.AbstractLog;
import org.apache.hadoop.hbase.regionserver.wal.filesystem.FSHLog;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A WAL Provider that returns a single thread safe WAL that writes to Hadoop FS.
 * By default, this implementation picks a directory in Hadoop FS based on a combination of
 * <ul>
 *   <li>the HBase root WAL directory
 *   <li>HConstants.HREGION_LOGDIR_NAME
 *   <li>the given factory's factoryId (usually identifying the regionserver by host:port)
 * </ul>
 * It also uses the providerId to diffentiate among files.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultWALProvider extends AbstractWALProvider {
  private static final Log LOG = LogFactory.getLog(DefaultWALProvider.class);

  @Override
  protected AbstractLog createWAL() throws IOException {
    return new FSHLog(FSUtils.getWALFileSystem(conf), FSUtils.getWALRootDir(conf),
      WALUtils.getWALDirectoryName(factory.factoryId), HConstants.HREGION_OLDLOGDIR_NAME,
      conf, listeners, true, logPrefix,
      META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
  }
}
