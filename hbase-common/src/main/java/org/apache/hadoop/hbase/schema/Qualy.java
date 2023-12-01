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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class Qualy extends AbstractSchemaComponent {


  // we ignore this parameter for any comparison and hashcode on purpose
  private ColumnType type;

  public Qualy(String qual) {
    this(Bytes.toBytes(qual));
  }

  public Qualy(byte[] qual) {
    this(qual, 0, qual.length);
  }

  public Qualy(byte[] qual, int offset, int length) {
    super(qual, offset, length);
    type = ColumnType.NONE;
  }

  public void updateType(ColumnType type) {
    this.type = type;
  }

  public ColumnType getType() {
    return type;
  }

  @Override
  public String toString() {
    return super.toString() + ": " + type.name();
  }

}
