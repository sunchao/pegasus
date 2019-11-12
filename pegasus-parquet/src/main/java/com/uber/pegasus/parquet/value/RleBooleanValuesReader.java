package com.uber.pegasus.parquet.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;

public class RleBooleanValuesReader extends AbstractRleValuesReader<BitVector> {
  public RleBooleanValuesReader(BufferAllocator allocator) {
    super(allocator);
  }

  @Override
  public boolean readBoolean() {
    return readNextInt() != 0;
  }

  @Override
  public void read(BitVector c, int rowId) {
    c.set(rowId, readNextInt() != 0 ? 1 : 0);
  }

  @Override
  public void readBatch(BitVector c, int rowId, int total) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < n; i++) {
            c.set(rowId + i, currentValue);
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
