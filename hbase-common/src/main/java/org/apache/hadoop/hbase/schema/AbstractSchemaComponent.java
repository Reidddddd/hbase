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
package org.apache.hadoop.hbase.schema;

import java.util.Comparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractSchemaComponent implements Comparable<AbstractSchemaComponent>,
    Comparator<AbstractSchemaComponent> {
  private final byte[] bytes;
  private final int offset;
  private final int length;

  AbstractSchemaComponent(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AbstractSchemaComponent)) {
      return false;
    }
    AbstractSchemaComponent otherComponent = (AbstractSchemaComponent) other;
    return Bytes.equals(bytes, offset, length, otherComponent.bytes, otherComponent.offset,
      otherComponent.length);
  }

  @Override
  public int compareTo(AbstractSchemaComponent other) {
    if (this == other) {
      return 0;
    }
    return Bytes.compareTo(bytes, offset, length, other.bytes, other.offset, other.length);
  }

  @Override
  public int compare(AbstractSchemaComponent o1, AbstractSchemaComponent o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(bytes, offset, length);
  }



  @Override
  public String toString() {
    return Bytes.toString(bytes, offset, length);
  }

  public byte[] cloneContent() {
    byte[] replica = new byte[length];
    System.arraycopy(bytes, offset, replica, 0, length);
    return replica;
  }
}
