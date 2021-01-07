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
package com.dremio.sabot.op.filter;

import java.util.ArrayList;

import javax.inject.Named;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;

/* TODO: Remove shared code with FilterTemplate2 */
public abstract class FilterTemplateAccelerated implements Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterTemplate2.class);

  private SelectionVector4 outgoingSelectionVector;
  private SelectionVector2 incomingSelectionVector;
  private SelectionVectorMode svMode;

  private VectorAccessible incomingVec;

  // Load native library libNativeFilter.so
  static {
    System.load("/usr/lib64/libNativeFilter.so");
  }

  private native int doNativeEval(byte[] schema, int numberOfRecords, long[] inBufAddresses, long[] inBufSizes);

  @Override
  public void setup(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) throws SchemaChangeException {
    // this.outgoingSelectionVector = outgoing.getSelectionVector2();
    this.outgoingSelectionVector = outgoing.getSelectionVector4();
    this.svMode = incoming.getSchema().getSelectionVectorMode();
    this.incomingVec = incoming;

    switch(svMode){
      case NONE:
        break;
      case TWO_BYTE:
        this.incomingSelectionVector = incoming.getSelectionVector2();
        break;
      default:
        // SV4 is handled in FilterTemplate4
        throw new UnsupportedOperationException();
    }
    doSetup(context, incoming, outgoing);
  }

  public int filterBatch(int recordCount){
    if (recordCount == 0) {
      return 0;
    }

    outgoingSelectionVector.allocateNew(recordCount);

    final int outputRecords;
    switch(svMode){
      case NONE:
        outputRecords = filterBatchNoSV(recordCount);
        break;
      case TWO_BYTE:
        outputRecords = filterBatchSV2(recordCount);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return outputRecords;
  }

  private int filterBatchSV2(int recordCount){
    int svIndex = 0;
    final int count = recordCount;
    for(int i = 0; i < count; i++){
      char index = incomingSelectionVector.getIndex(i);
      if(true){
        outgoingSelectionVector.setIndex(svIndex, index);
        svIndex++;
      }
    }
    outgoingSelectionVector.setRecordCount(svIndex);
    return svIndex;
  }

  private int filterBatchNoSV(int recordCount){
    int svIndex = 0;
    for(int i = 0; i < recordCount; i++){
      if(true){
        outgoingSelectionVector.setIndex(svIndex, i);
        svIndex++;
      }
    }
    outgoingSelectionVector.setRecordCount(svIndex);

    ArrayList<Byte> byteArr = new ArrayList<Byte>();
    for (int i = 0; i < svIndex; i++) {
      byteArr.add(outgoingSelectionVector.getBuffer(false).getByte(i));
    }

    System.out.println(byteArr);

    return svIndex;

    // Cast incoming VectorAccessible to VectorContainer for easier access to the arrow buffers
//    VectorContainer in = (VectorContainer) this.incomingVec;
//
//    // Extract input buffers
//    ArrowBuf[] tripSecondsBuffers = in.getValueAccessorById(ValueVector.class, 0).getValueVector().getBuffers(false);
//    ArrowBuf[] companyBuffers = in.getValueAccessorById(ValueVector.class, 1).getValueVector().getBuffers(false);
//    ArrowBuf[] buffers = (ArrowBuf[]) org.apache.commons.lang.ArrayUtils.addAll(tripSecondsBuffers, companyBuffers);
//
//    // Extract schema and save as byte array
//    BatchSchema schema = in.getSchema();
//    byte[] schemaAsBytes = new byte[0];
//    try {
//      schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//
//    // Compute buffer addresses and sizes for input buffers
//    long[] inBufAddresses = Stream.of(buffers).mapToLong(b -> b.memoryAddress()).toArray();
//    long[] inBufSizes = Stream.of(buffers).mapToLong(b -> b.readableBytes()).toArray();
//
//    // Add selectionVector buffer to buffers array
//    ArrowBuf svBuffer = outgoingSelectionVector.getBuffer();
//    ArrowBuf[] allBuffers = (ArrowBuf[]) org.apache.commons.lang.ArrayUtils.addAll(buffers, new ArrowBuf[]{svBuffer});
//    long[] bufAddresses = (long[]) org.apache.commons.lang.ArrayUtils.addAll(inBufAddresses, new long[]{svBuffer.memoryAddress()});
//    long[] bufSizes = (long[]) org.apache.commons.lang.ArrayUtils.addAll(inBufSizes, new long[]{svBuffer.writableBytes()});
//
//    // Call native function, should return number of matched records
//    int numberOfRecords = doNativeEval(schemaAsBytes, recordCount, bufAddresses, bufSizes);
//
//    outgoingSelectionVector.setRecordCount(numberOfRecords);
//    return numberOfRecords;
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
