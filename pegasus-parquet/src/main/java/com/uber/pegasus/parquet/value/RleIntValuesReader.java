package com.uber.pegasus.parquet.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;

public class RleIntValuesReader extends AbstractRleValuesReader<IntVector> {
  public RleIntValuesReader(BufferAllocator allocator) {
    super(allocator);
  }
  public RleIntValuesReader(BufferAllocator allocator, int bitWidth) {
    super(allocator, bitWidth);
  }

  public RleIntValuesReader(BufferAllocator allocator, int bitWidth, boolean readLength) {
    super(allocator, bitWidth, readLength);
  }

  @Override
  public int readInteger() {
    return readNextInt();
  }

  @Override
  public void read(IntVector c, int rowId) {
    c.set(rowId, readNextInt());
  }

  @Override
  public void readBatch(IntVector c, int rowId, int total) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < n; i++) {
            c.set(rowId + i, this.currentValue);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; i++) {
            c.set(rowId + i, currentBuffer[currentBufferIdx++]);
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }
}
