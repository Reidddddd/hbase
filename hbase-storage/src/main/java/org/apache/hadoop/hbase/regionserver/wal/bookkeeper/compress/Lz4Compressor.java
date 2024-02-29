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
package org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress;

import dlshade.net.jpountz.lz4.LZ4Compressor;
import dlshade.net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class Lz4Compressor implements LedgerEntryCompressor {
  private final ThreadLocal<LZ4Compressor> compressorThreadLocal = new ThreadLocal<>();

  @Override
  public byte[] compress(byte[] uncompressed) {
    LZ4Compressor compressor = compressorThreadLocal.get();
    if (compressor == null) {
      compressor = LZ4Factory.fastestInstance().fastCompressor();
      compressorThreadLocal.set(compressor);
    }
    // Use fast lz4 compression, the compressed format is: int (4 bytes) + compressed bytes
    int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length);
    byte[] compressed = new byte[maxCompressedLength + Bytes.SIZEOF_INT];
    compressor.compress(uncompressed, 0, uncompressed.length, compressed,
      Bytes.SIZEOF_INT, maxCompressedLength);
    byte[] header = Bytes.toBytes(uncompressed.length);
    Bytes.putBytes(compressed, 0, header, 0, Bytes.SIZEOF_INT);

    return compressed;
  }
}
