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
package org.apache.hadoop.hbase.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.IPCReservoir;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Codec that does KeyValue version 1 serialization.
 *
 * <p>Encodes Cell as serialized in KeyValue with total length prefix.
 * This is how KVs were serialized in Puts, Deletes and Results pre-0.96.  Its what would
 * happen if you called the Writable#write KeyValue implementation.  This encoder will fail
 * if the passed Cell is not an old-school pre-0.96 KeyValue.  Does not copy bytes writing.
 * It just writes them direct to the passed stream.
 *
 * <p>If you wrote two KeyValues to this encoder, it would look like this in the stream:
 * <pre>
 * length-of-KeyValue1 // A java int with the length of KeyValue1 backing array
 * KeyValue1 backing array filled with a KeyValue serialized in its particular format
 * length-of-KeyValue2
 * KeyValue2 backing array
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class KeyValueCodec extends AbstractDecoder {
  public static class KeyValueEncoder extends BaseEncoder {
    public KeyValueEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Do not write tags over RPC
      KeyValueUtil.oswrite(cell, out, false);
    }
  }

  public static class KeyValueDecoder extends BaseDecoder {
    public KeyValueDecoder(final InputStream in) {
      super(in);
    }

    @Override
    protected Cell parseCell() throws IOException {
      return KeyValueUtil.iscreate(in, false);
    }
  }

  /**
   * Implementation depends on {@link InputStream#available()}
   */
  @Override
  public Decoder getDecoder(final InputStream is) {
    return new KeyValueDecoder(is);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }

  @InterfaceAudience.Private
  @Override
  public Decoder getDecoder(ByteBuffer buf, BytesGenerator generator) {
    return new ByteBufferKeyValueDecoder(buf, generator);
  }

  @InterfaceAudience.Private
  public static class ByteBufferKeyValueDecoder implements Codec.Decoder  {
    final IPCReservoir reservoir;
    final BytesGenerator generator;
    Cell current;
    ByteBuffer buf;
    boolean remaining;

    public ByteBufferKeyValueDecoder(ByteBuffer buf, BytesGenerator generator) {
      this.buf = buf;
      this.reservoir = IPCReservoir.getInstance();
      this.generator = generator;
      this.remaining = true;
    }

    @Override
    public Cell current() {
      return current;
    }

    @Override
    public boolean advance() throws IOException {
      if (!remaining) {
        return false;
      }
      try {
        int len = buf.getInt();
        int currentPos = buf.position();
        byte[] kv = generator.getBytes(len);
        int offset = generator.getOffset();
        ByteBufferUtils.copyFromBufferToArray(kv, buf, currentPos, offset, len);
        current = new KeyValue(kv, offset, len);
        buf.position(currentPos + len);
        return true;
      } finally {
        remaining = buf.hasRemaining();
        if (!remaining) {
          reservoir.reclaimBuffer(buf);
          buf = null;
        }
      }
    }
  }

}
