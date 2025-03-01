/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.physical.base;

import com.dremio.exec.physical.config.AbstractSort;
import com.dremio.exec.physical.config.AcceleratedFilter;
import com.dremio.exec.physical.config.BroadcastSender;
import com.dremio.exec.physical.config.DictionaryLookupPOP;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.physical.config.FlattenPOP;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.exec.physical.config.Limit;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.exec.physical.config.MergingReceiverPOP;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.physical.config.RoundRobinSender;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.physical.config.SingleSender;
import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.physical.config.UnionExchange;
import com.dremio.exec.physical.config.UnorderedReceiver;
import com.dremio.exec.physical.config.Values;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPOP;

/**
 * Visitor class designed to traversal of a operator tree.  Basis for a number of operator manipulations including fragmentation and materialization.
 * @param <RETURN> The class associated with the return of each visit method.
 * @param <EXTRA> The class object associated with additional data required for a particular operator modification.
 * @param <EXCEP> An optional exception class that can be thrown when a portion of a modification or traversal fails.  Must extend Throwable.  In the case where the visitor does not throw any caught exception, this can be set as RuntimeException.
 */
public interface PhysicalVisitor<RETURN, EXTRA, EXCEP extends Throwable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalVisitor.class);


  public RETURN visitExchange(Exchange exchange, EXTRA value) throws EXCEP;
  public RETURN visitGroupScan(GroupScan groupScan, EXTRA value) throws EXCEP;
  public RETURN visitSubScan(SubScan subScan, EXTRA value) throws EXCEP;
  public RETURN visitStore(Store store, EXTRA value) throws EXCEP;

  public RETURN visitFilter(Filter filter, EXTRA value) throws EXCEP;
  public RETURN visitAcceleratedFilter(AcceleratedFilter filter, EXTRA value) throws EXCEP;
  public RETURN visitUnion(UnionAll union, EXTRA value) throws EXCEP;
  public RETURN visitProject(Project project, EXTRA value) throws EXCEP;
  public RETURN visitDictionaryLookup(DictionaryLookupPOP dictionaryLookupPOP, EXTRA value) throws EXCEP;
  public RETURN visitSort(AbstractSort sort, EXTRA value) throws EXCEP;
  public RETURN visitLimit(Limit limit, EXTRA value) throws EXCEP;
  public RETURN visitFlatten(FlattenPOP flatten, EXTRA value) throws EXCEP;
  public RETURN visitMergeJoin(MergeJoinPOP join, EXTRA value) throws EXCEP;
  public RETURN visitHashJoin(HashJoinPOP join, EXTRA value) throws EXCEP;
  public RETURN visitWriterCommiter(WriterCommitterPOP commiter, EXTRA value) throws EXCEP;
  public RETURN visitNestedLoopJoin(NestedLoopJoinPOP join, EXTRA value) throws EXCEP;
  public RETURN visitSender(Sender sender, EXTRA value) throws EXCEP;
  public RETURN visitReceiver(Receiver receiver, EXTRA value) throws EXCEP;
  public RETURN visitStreamingAggregate(StreamingAggregate agg, EXTRA value) throws EXCEP;
  public RETURN visitHashAggregate(HashAggregate agg, EXTRA value) throws EXCEP;
  public RETURN visitWriter(Writer op, EXTRA value) throws EXCEP;
  public RETURN visitValues(Values op, EXTRA value) throws EXCEP;
  public RETURN visitEmptyValues(EmptyValues op, EXTRA value) throws EXCEP;
  public RETURN visitOp(PhysicalOperator op, EXTRA value) throws EXCEP;

  public RETURN visitHashPartitionSender(HashPartitionSender op, EXTRA value) throws EXCEP;
  public RETURN visitUnorderedReceiver(UnorderedReceiver op, EXTRA value) throws EXCEP;
  public RETURN visitMergingReceiver(MergingReceiverPOP op, EXTRA value) throws EXCEP;
  public RETURN visitHashPartitionSender(HashToRandomExchange op, EXTRA value) throws EXCEP;
  public RETURN visitBroadcastSender(BroadcastSender op, EXTRA value) throws EXCEP;
  public RETURN visitRoundRobinSender(RoundRobinSender op, EXTRA value) throws EXCEP;
  public RETURN visitScreen(Screen op, EXTRA value) throws EXCEP;
  public RETURN visitSingleSender(SingleSender op, EXTRA value) throws EXCEP;
  public RETURN visitUnionExchange(UnionExchange op, EXTRA value) throws EXCEP;
  public RETURN visitWindowFrame(WindowPOP op, EXTRA value) throws EXCEP;
  public RETURN visitConvertFromJson(ConvertFromJsonPOP op, EXTRA value) throws EXCEP;
}
