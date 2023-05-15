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
package org.apache.hadoop.hbase.trace;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to create the graph formed by spans.
 */
@InterfaceAudience.Private
public class TraceTree {

  @InterfaceAudience.Private
  public static class SpansByParent {

    private static final Comparator<Span> COMPARATOR = Comparator.comparing(Span::getSpanId);
    private final TreeSet<Span> treeSet;

    private final HashMap<SpanId, LinkedList<Span>> parentToSpans;

    SpansByParent(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<>(COMPARATOR);
      parentToSpans = new HashMap<>();

      for (Span span : spans) {
        treeSet.add(span);
        for (SpanId parent : span.getParents()) {
          LinkedList<Span> list = parentToSpans.computeIfAbsent(parent, k -> new LinkedList<>());
          list.add(span);
        }
        if (span.getParents().length == 0) {
          LinkedList<Span> list =
            parentToSpans.computeIfAbsent(SpanId.INVALID, k -> new LinkedList<>());
          list.add(span);
        }
      }
      this.treeSet = treeSet;
    }

    public List<Span> find(SpanId parentId) {
      LinkedList<Span> spans = parentToSpans.get(parentId);
      if (spans == null) {
        return new LinkedList<>();
      }
      return spans;
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
    }
  }


  @InterfaceAudience.Private
  public static class SpansByProcessId {
    private static final Comparator<Span> COMPARATOR = Comparator.comparing(Span::getSpanId);
    private final TreeSet<Span> treeSet;

    SpansByProcessId(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<>(COMPARATOR);
      treeSet.addAll(spans);
      this.treeSet = treeSet;
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
    }
  }

  private final SpansByParent spansByParent;
  private final SpansByProcessId spansByProcessId;

  /**
   * Create a new TraceTree
   * @param spans The collection of spans to use to create this TraceTree. Should
   *              have at least one root span.
   */
  public TraceTree(Collection<Span> spans) {
    if (spans == null) {
      spans = Collections.emptySet();
    }
    this.spansByParent = new SpansByParent(spans);
    this.spansByProcessId = new SpansByProcessId(spans);
  }

  public SpansByParent getSpansByParent() {
    return spansByParent;
  }

  public SpansByProcessId getSpansByProcessId() {
    return spansByProcessId;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (Iterator<Span> iter = spansByParent.iterator(); iter.hasNext();) {
      Span span = iter.next();
      bld.append(prefix).append(span.toString());
      prefix = "\n";
    }
    return bld.toString();
  }
}
