/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class AccumuloRangeGenerator implements NodeProcessor {
  private static final Logger log = LoggerFactory.getLogger(AccumuloRangeGenerator.class);

  private AccumuloPredicateHandler predicateHandler;
  private String hiveRowIdColumnName;

  public AccumuloRangeGenerator(AccumuloPredicateHandler predicateHandler, String hiveRowIdColumnName) {
    this.predicateHandler = predicateHandler;
    this.hiveRowIdColumnName = hiveRowIdColumnName;
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
//    System.out.println("Node: " + nd);
//    System.out.println("Stack: " + stack);
//    System.out.println("nodeOutputs: " + Arrays.toString(nodeOutputs));
//    System.out.println();

    // If it's not some operator, pass it back
    if (!(nd instanceof ExprNodeGenericFuncDesc)) {
      return nd;
    }

    ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) nd;

    // 'and' nodes need to be intersected
    if (FunctionRegistry.isOpAnd(func)) {
      // We might have multiple ranges coming from children
      List<Range> andRanges = new ArrayList<Range>();

      for (Object nodeOutput : nodeOutputs) {
        // null signifies nodes that are irrelevant to the generation
        // of Accumulo Ranges
        if (null == nodeOutput) {
          continue;
        }

        // The child is a single Range
        if (nodeOutput instanceof Range) {
          Range childRange = (Range) nodeOutput;

          // No existing ranges, just accept the current
          if (andRanges.isEmpty()) {
            andRanges.add(childRange);
          } else {
            // For each range we have, intersect them. If they don't overlap
            // the range can be discarded
            List<Range> newRanges = new ArrayList<Range>();
            for (Range andRange : andRanges) {
              Range intersectedRange = andRange.clip(childRange, true);
              if (null != intersectedRange) {
                newRanges.add(intersectedRange);
              }
            }

            // Set the newly-constructed ranges as the current state
            andRanges = newRanges;
          }
        } else if (nodeOutput instanceof List) {
          @SuppressWarnings("unchecked")
          List<Range> childRanges = (List<Range>) nodeOutput;

          // No ranges, use the ranges from the child
          if (andRanges.isEmpty()) {
            andRanges.addAll(childRanges);
          } else {
            List<Range> newRanges = new ArrayList<Range>();

            // Cartesian product of our ranges, to the child ranges
            for (Range andRange : andRanges) {
              for (Range childRange : childRanges) {
                Range intersectedRange = andRange.clip(childRange, true);

                // Retain only valid intersections (discard disjoint ranges)
                if (null != intersectedRange) {
                  newRanges.add(intersectedRange);
                }
              }
            }

            // Set the newly-constructed ranges as the current state
            andRanges = newRanges;
          }
        } else {
          log.error("Expected Range from {} but got {}", nd, nodeOutput);
          throw new IllegalArgumentException("Expected Range but got " + nodeOutput.getClass().getName());
        }
      }

      return andRanges;
    // 'or' nodes need to be merged
    } else if (FunctionRegistry.isOpOr(func)) {
      List<Range> orRanges = new ArrayList<Range>(nodeOutputs.length);
      for (Object nodeOutput : nodeOutputs) {
        if (nodeOutput instanceof Range) {
          orRanges.add((Range) nodeOutput);
        } else if (nodeOutput instanceof List) {
          @SuppressWarnings("unchecked")
          List<Range> childRanges = (List<Range>) nodeOutput;
          orRanges.addAll(childRanges);
        } else {
          log.error("Expected Range from " + nd + " but got " + nodeOutput);
          throw new IllegalArgumentException("Expected Range but got " + nodeOutput.getClass().getName());
        }
      }

      // Try to merge multiple ranges together
      if (orRanges.size() > 1) {
        return Range.mergeOverlapping(orRanges);
      } else if (1 == orRanges.size()){
        // Return just the single Range
        return orRanges.get(0);
      } else {
        // No ranges, just return the empty list
        return orRanges;
      }
    } else if (FunctionRegistry.isOpNot(func)) {
      // TODO handle negations
      throw new IllegalArgumentException("Not yet implemented");
    } else {
      // a binary operator (gt, lt, ge, le, eq, ne)
      GenericUDF genericUdf = func.getGenericUDF();

      // Find the argument to the operator which is a constant
      ExprNodeConstantDesc constantDesc = null;
      ExprNodeColumnDesc columnDesc = null;
      for (Object nodeOutput : nodeOutputs) {
        if (nodeOutput instanceof ExprNodeConstantDesc) {
          constantDesc = (ExprNodeConstantDesc) nodeOutput;
        } else if (nodeOutput instanceof ExprNodeColumnDesc) {
          columnDesc = (ExprNodeColumnDesc) nodeOutput;
        }
      }

      // TODO What if it's actually constant = constant or column = column
      if (null == constantDesc || null == columnDesc) {
        StringBuilder sb = new StringBuilder(32);
        if (null == constantDesc) {
          sb.append("no constant");
        }
        if (null == columnDesc) {
          if (null == constantDesc) {
            sb.append(" and ");
          }
          sb.append("no column");
        }

        throw new IllegalArgumentException("Expected a column and a constant but found " + sb.toString());
      }

      // Reject any clauses that are against a column that isn't the rowId mapping
      if (!this.hiveRowIdColumnName.equals(columnDesc.getColumn())) {
        return null;
      }

      ConstantObjectInspector objInspector = constantDesc.getWritableObjectInspector();
      String constant = objInspector.getWritableConstantValue().toString();
      Text constText = new Text(constant);

      Class<? extends CompareOp> opClz;
      try {
        opClz = predicateHandler.getCompareOpClass(genericUdf.getUdfName());
      } catch (NoSuchCompareOpException e) {
        throw new IllegalArgumentException("Unhandled UDF class: " + genericUdf.getUdfName());
      }

      CompareOp compareOp;
      try {
        compareOp = opClz.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Could not instantiate " + opClz);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Could not instantiate " + opClz);
      }

      if (compareOp instanceof Equal) {
        return new Range(constText, true, constText, true); // start inclusive to end inclusive
      } else if (compareOp instanceof GreaterThanOrEqual) {
        return new Range(constText, null); // start inclusive to infinity inclusive
      } else if (compareOp instanceof GreaterThan) {
        return new Range(constText, false, null, true); // start exclusive to infinity inclusive
      } else if (compareOp instanceof LessThanOrEqual) {
        return new Range(null, true, constText, true); // neg-infinity to start inclusive
      } else if (compareOp instanceof LessThan) {
        return new Range(null, true, constText, false); // neg-infinity to start exclusive
      } else {
        throw new IllegalArgumentException("Could not process " + compareOp);
      }
    }
  }
}
