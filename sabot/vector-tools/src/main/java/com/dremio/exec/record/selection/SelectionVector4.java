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
package com.dremio.exec.record.selection;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.record.DeadBuf;
import com.google.common.base.Preconditions;

public class SelectionVector4 implements AutoCloseable {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVector4.class);

  private ArrowBuf data;
  private int recordCount;
  private int start;
  private int length;
  private BufferAllocator allocator = null;

  public SelectionVector4(ArrowBuf vector, int recordCount, int batchRecordCount) {
    Preconditions.checkArgument(recordCount < Integer.MAX_VALUE / 4,
        "Currently, Dremio can only support allocations up to 2gb in size.  "
        + "You requested an allocation of %s bytes.", recordCount * 4);
    this.recordCount = recordCount;
    this.start = 0;
    this.length = Math.min(batchRecordCount, recordCount);
    this.data = vector;
  }

  // Overload the constructor to enable passing in a buffer allocator
  public SelectionVector4(ArrowBuf vector, int recordCount, int batchRecordCount, BufferAllocator allocator) {
    Preconditions.checkArgument(recordCount < Integer.MAX_VALUE / 4,
      "Currently, Dremio can only support allocations up to 2gb in size.  "
        + "You requested an allocation of %s bytes.", recordCount * 4);
    this.recordCount = recordCount;
    this.start = 0;
    this.length = Math.min(batchRecordCount, recordCount);
    this.data = vector;
    this.allocator = allocator;
  }

  public int getTotalCount() {
    return recordCount;
  }

  public int getCount() {
    return length;
  }

  /**
   * Get location of current start point.
   * @return Memory address where current batch starts.
   */
  public long getMemoryAddress() {
    return data.memoryAddress() + start * 4;
  }

  public void set(int index, int compound) {
    data.setInt(index*4, compound);
  }

  public void set(int index, int recordBatch, int recordIndex) {
    data.setInt(index*4, (recordBatch << 16) | (recordIndex & 65535));
  }

  public int get(int index) {
    return data.getInt( (start+index)*4);
  }

  /**
   * Caution: This method shares the underlying buffer between this vector and the newly created one.
   * @param batchRecordCount this will be used when creating the new vector
   * @return Newly created single batch SelectionVector4.
   */
  public SelectionVector4 createNewWrapperCurrent(int batchRecordCount) {
    data.retain();
    final SelectionVector4 sv4 = new SelectionVector4(data, recordCount, batchRecordCount);
    sv4.start = this.start;
    return sv4;
  }

  /**
   * Caution: This method shares the underlying buffer between this vector and the newly created one.
   * @return Newly created single batch SelectionVector4.
   */
  public SelectionVector4 createNewWrapperCurrent() {
    return createNewWrapperCurrent(length);
  }

  public boolean next() {
//    logger.debug("Next called. Start: {}, Length: {}, recordCount: " + recordCount, start, length);

    if (start + length >= recordCount) {

      start = recordCount;
      length = 0;
//      logger.debug("Setting count to zero.");
      return false;
    }

    start = start+length;
    int newEnd = Math.min(start+length, recordCount);
    length = newEnd - start;
//    logger.debug("New start {}, new length {}", start, length);
    return true;
  }

  public void clear() {
    start = 0;
    length = 0;
    if (data != DeadBuf.DEAD_BUFFER) {
      data.release();
      data = DeadBuf.DEAD_BUFFER;
    }
  }

  public void allocateNew(int size) throws Exception {

    // Can only be used if a buffer allocator has been set
    if (allocator != null) {
      clear();
      data = allocator.buffer(size * 4);
    } else {
      throw new Exception("No buffer allocator set");
    }

  }

  // Write an index of a matching record into the selection vector
  public void setIndex(int sv_index, int index) {
    data.setInt(sv_index * 4, index);
  }

  // Caution: this method name is made to match the same method in the SelectionVector2 implementation
  // to enable the two implementations to be easily swapped. However, in the SV4 implementation, this
  // method sets the length and not the recordCount.
  public void setRecordCount(int recordCount){
    this.length = recordCount;
  }

  public long memoryAddress(){
    return data.memoryAddress();
  }

  public ArrowBuf getBuffer(boolean clear) {
    ArrowBuf bufferHandle = this.data;

    if (clear) {
      /* Increment the ref count for this buffer */
      bufferHandle.retain(1);

      /* We are passing ownership of the buffer to the
       * caller. clear the buffer from within our selection vector
       */
      clear();
    }

    return bufferHandle;
  }

  @Override
  public void close() {
    clear();
  }
}
