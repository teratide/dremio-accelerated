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

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;

/**
 * VectorContainers with sv2 or sv4 (filters).
 */
public class VectorContainerWithSV extends VectorContainer {

  private final SelectionVectorMode selectionVectorMode;

  private final SelectionVector2 sv2;
  private final SelectionVector4 sv4;

  public VectorContainerWithSV(BufferAllocator allocator, SelectionVector2 sv2) {
    super(allocator);
    this.selectionVectorMode = SelectionVectorMode.TWO_BYTE;
    this.sv2 = sv2;
    this.sv4 = null;  // Gives nullPointerException when accidentally used
  }

  public VectorContainerWithSV(BufferAllocator allocator, SelectionVector4 sv4) {
    super(allocator);
    this.selectionVectorMode = SelectionVectorMode.FOUR_BYTE;
    this.sv4 = sv4;
    this.sv2 = null;  // Gives nullPointerException when accidentally used
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  @Override
  public void close() {
    if (this.selectionVectorMode == SelectionVectorMode.TWO_BYTE) {
      sv2.clear();
    } else {
      sv4.clear();
    }
    super.close();
  }

}
