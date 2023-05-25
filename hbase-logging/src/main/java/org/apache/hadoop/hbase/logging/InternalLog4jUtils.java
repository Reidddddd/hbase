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
package org.apache.hadoop.hbase.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The actual class for operating on log4j.
 * <p/>
 * This class will depend on log4j directly, so callers should not use this class directly to avoid
 * introducing log4j dependencies to downstream users. Please call the methods in
 * {@link Log4jUtils}, as they will call the methods here through reflection.
 */
@InterfaceAudience.Private
final class InternalLog4jUtils {

  private InternalLog4jUtils() {
  }

  static void setLogLevel(String loggerName, String levelName) {
    org.apache.log4j.Logger logger = org.apache.log4j.LogManager.getLogger(loggerName);
    org.apache.log4j.Level level = org.apache.log4j.Level.toLevel(levelName.toUpperCase());
    if (!level.toString().equalsIgnoreCase(levelName)) {
      throw new IllegalArgumentException("Unsupported log level " + levelName);
    }
    logger.setLevel(level);
  }

  static String getEffectiveLevel(String loggerName) {
    org.apache.log4j.Logger logger = org.apache.log4j.LogManager.getLogger(loggerName);
    return logger.getEffectiveLevel().toString();
  }

  static Set<File> getActiveLogFiles() throws IOException {
    Set<File> ret = new HashSet<>();
    org.apache.log4j.Appender a;
    @SuppressWarnings("unchecked")
    Enumeration<org.apache.log4j.Appender> e =
      org.apache.log4j.Logger.getRootLogger().getAllAppenders();
    while (e.hasMoreElements()) {
      a = e.nextElement();
      if (a instanceof org.apache.log4j.FileAppender) {
        org.apache.log4j.FileAppender fa = (org.apache.log4j.FileAppender) a;
        String filename = fa.getFile();
        ret.add(new File(filename));
      }
    }
    return ret;
  }
  
  static void enableAsyncAuditLog(String loggerName,
                                  boolean blocking,  int asyncAppenderBufferSize) {
    org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(loggerName);
    @SuppressWarnings("unchecked") List<org.apache.log4j.Appender> appenders =
        Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0)
        instanceof org.apache.log4j.AsyncAppender)) {
      org.apache.log4j.AsyncAppender asyncAppender = new org.apache.log4j.AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (org.apache.log4j.Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      // non-blocking so that server will not wait for async logger
      // even when the appender's buffer is full
      // some audit events will be lost in this case
      asyncAppender.setBlocking(blocking);
      asyncAppender.setBufferSize(asyncAppenderBufferSize);
      logger.addAppender(asyncAppender);
    }
  }
}
