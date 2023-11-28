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
import org.apache.yetus.audience.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class Famy implements Comparable<Famy>, Comparator<Famy> {

  private final byte[] family;

  public Famy(String famy) {
    this(Bytes.toBytes(famy));
  }

  public Famy(byte[] famy) {
    family = famy;
  }

  public byte[] getFamily() {
    return family;
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(family);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Famy)) {
      return false;
    }
    return Bytes.equals(family, ((Famy) other).family);
  }

  @Override
  public int compareTo(Famy other) {
    if (this == other) {
      return 0;
    }
    return Bytes.compareTo(family, other.family);
  }

  @Override
  public int compare(Famy o1, Famy o2) {
    return o1.compareTo(o2);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.family);
  }
}
