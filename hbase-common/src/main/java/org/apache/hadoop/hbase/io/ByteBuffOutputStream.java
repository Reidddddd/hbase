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

package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * Different from ByteBufferOutputStream. It is IA.Private.
 */
@InterfaceAudience.Private
public class ByteBuffOutputStream extends OutputStream {

  // Borrowed from openJDK:
  // http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/8-b132/java/util/ArrayList.java#221
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  protected ByteBuffer buf;

  private ByteBuffPool pool;

  public ByteBuffOutputStream(final ByteBuffer bb, final ByteBuffPool pool) {
    this.buf = bb;
    this.buf.clear();
    this.pool = pool;
  }

  public int size() {
    return buf.position();
  }

  /**
   * This flips the underlying BB so be sure to use it _last_!
   * @return ByteBuffer
   */
  public ByteBuffer getByteBuffer() {
    buf.flip();
    return buf;
  }

  private void checkSizeAndGrow(int extra) {
    int capacityNeeded = buf.position() + extra;
    if (capacityNeeded > buf.limit()) {
      // guarantee it's possible to fit
      if (capacityNeeded > MAX_ARRAY_SIZE) {
        throw new BufferOverflowException();
      }
      ByteBuffer newBuf = pool.claimBuffer(capacityNeeded);
      buf.flip();
      newBuf.put(buf);
      pool.reclaimBuffer(buf);
      buf = newBuf;
    }
  }

  // OutputStream
  @Override
  public void write(int b) throws IOException {
    checkSizeAndGrow(Bytes.SIZEOF_BYTE);
    buf.put((byte)b);
  }

  /**
   * Writes the complete contents of this byte buffer output stream to
   * the specified output stream argument.
   *
   * @param      out   the output stream to which to write the data.
   * @exception  IOException  if an I/O error occurs.
   */
  public synchronized void writeTo(OutputStream out) throws IOException {
    WritableByteChannel channel = Channels.newChannel(out);
    ByteBuffer bb = buf.duplicate();
    bb.flip();
    channel.write(bb);
  }

  @Override
  public void write(byte[] b) throws IOException {
    checkSizeAndGrow(b.length);
    buf.put(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkSizeAndGrow(len);
    buf.put(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    // noop again. heh
  }

  public byte[] toByteArray(int offset, int length) {
    ByteBuffer bb = buf.duplicate();
    bb.flip();

    byte[] chunk = new byte[length];

    bb.position(offset);
    bb.get(chunk, 0, length);
    return chunk;
  }

}
