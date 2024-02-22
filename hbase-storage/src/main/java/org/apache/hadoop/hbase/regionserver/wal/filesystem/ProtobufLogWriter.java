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
package org.apache.hadoop.hbase.regionserver.wal.filesystem;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufLogWriter;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.FileSystemBasedWriter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A hdfs based {@link AbstractProtobufLogWriter}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ProtobufLogWriter extends AbstractProtobufLogWriter implements FileSystemBasedWriter {
  private static final Log LOG = LogFactory.getLog(ProtobufLogWriter.class);

  private FileSystem fs;
  private Path path;
  private boolean overwritable;

  @Override
  @SuppressWarnings("deprecation")
  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
    throws IOException {
    this.fs = fs;
    this.path = path;
    this.overwritable = overwritable;
    super.init(conf);
  }

  @Override
  protected void initOutput() throws IOException {
    int bufferSize = FSUtils.getDefaultBufferSize(fs);
    short replication = (short)conf.getInt(
      "hbase.regionserver.hlog.replication", FSUtils.getDefaultReplication(fs, path));
    long blockSize = conf.getLong("hbase.regionserver.hlog.blocksize",
      FSUtils.getDefaultBlockSize(fs, path));
    output = fs.createNonRecursive(path, overwritable, bufferSize, replication, blockSize, null);
  }

  @Override
  protected boolean checkRecoveredEdits() {
    return FSUtils.isRecoveredEdits(this.path);
  }

  @Override
  public void sync() throws IOException {
    FSDataOutputStream fsdos = (FSDataOutputStream) this.output;
    if (fsdos == null) {
      return; // Presume closed
    }
    fsdos.flush();
    fsdos.hflush();
  }

  @Override
  public long getLength() throws IOException {
    try {
      return ((FSDataOutputStream)this.output).getPos();
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  public FSDataOutputStream getStream() {
    return (FSDataOutputStream) this.output;
  }

  @Override
  protected String getURL() {
    return this.path.toString();
  }
}
