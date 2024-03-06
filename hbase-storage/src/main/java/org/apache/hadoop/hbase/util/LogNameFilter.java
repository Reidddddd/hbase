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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.wal.WALUtils.WAL_FILE_NAME_DELIMITER;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LogNameFilter {
  private final String logPrefix;
  private final String logSuffix;

  public LogNameFilter(String logPrefix, String logSuffix) {
    this.logPrefix = logPrefix;
    this.logSuffix = logSuffix;
  }

  public boolean accept(String logName) {
    // The path should start with dir/<prefix> and end with our suffix
    if (!logName.startsWith(logPrefix)) {
      return false;
    }
    if (logSuffix.isEmpty()) {
      // When suffix is empty, the log name should be in this format:
      // /.../.../severName.port.startCode/base64(serverName.port.startCode).timestamp
      // The timestamp is a long which is the milliseconds when the log is created
      // We need to ensure the filename ends with a timestamp
      return org.apache.commons.lang.StringUtils.isNumeric(
        logName.substring(logPrefix.length() + WAL_FILE_NAME_DELIMITER.length()));
    }
    return logName.endsWith(logSuffix);
  }
}
