package com.dremio.sabot.op.filter;

import javax.inject.Named;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.FunctionContext;

/* TODO: Remove shared code with FilterTemplate2 */
public abstract class FilterTemplateAccelerated implements Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterTemplate2.class);

  private SelectionVector2 outgoingSelectionVector;
  private SelectionVector2 incomingSelectionVector;
  private SelectionVectorMode svMode;

  @Override
  public void setup(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) throws SchemaChangeException {
    this.outgoingSelectionVector = outgoing.getSelectionVector2();
    this.svMode = incoming.getSchema().getSelectionVectorMode();

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
        outgoingSelectionVector.setIndex(svIndex, (char)i);
        svIndex++;
      }
    }
    outgoingSelectionVector.setRecordCount(svIndex);
    return svIndex;
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
