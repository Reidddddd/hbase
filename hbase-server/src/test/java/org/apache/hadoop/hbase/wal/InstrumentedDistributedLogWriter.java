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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class InstrumentedDistributedLogWriter extends DistributedLogWriter {

  public static boolean activateFailure = false;

  public InstrumentedDistributedLogWriter() {
    super();
  }

  @Override
  public void append(Entry entry) throws IOException {
    super.append(entry);
    if (activateFailure && Bytes.equals(entry.getKey().getEncodedRegionName(),
      "break".getBytes())) {
      System.out.println(getClass().getName() + ": I will throw an exception now...");
      throw(new IOException("This exception is instrumented and should only be thrown for testing"
      ));
    }
  }

  @Override
  protected WALProtos.WALHeader buildWALHeader(Configuration conf,
    WALProtos.WALHeader.Builder builder) throws IOException {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(DistributedLogWriter.class.getSimpleName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf).getName());
    }
    return builder.build();
  }
}
