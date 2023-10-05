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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A class for client to update column type. Client shouldn't directly use it.
 * That's why annotate it Private
 * It should be attained via:
 * <pre>
 *   Column column = schema.getColumn(Famy, Qualy);
 * </pre>
 */
@InterfaceAudience.Private
public class MutableColumn extends Column {

  private final Schema schema;

  public MutableColumn(Schema schema, Famy famy, Qualy qualy) {
    super(famy, qualy);
    this.schema = schema;
  }

  public void updateType(ColumnType type) {
    super.updateType(type);
    schema.typeUpdatedColumns.add(this);
  }

}
