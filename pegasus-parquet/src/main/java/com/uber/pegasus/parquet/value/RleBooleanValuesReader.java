package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.BitVector;

public class RleBooleanValuesReader extends AbstractRleValuesReader<BitVector> {
  @Override
  public boolean readBoolean() {
    return readNextInt() != 0;
  }

  @Override
  public void readBatch(BitVector c, int rowId, int total, int maxDefLevel,
      ValuesReader<BitVector> data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            data.readBatch(c, rowId, n);
          } else {
            for (int i = 0; i < n; i++) {
              c.setNull(rowId + i);
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              c.set(rowId + i, data.readBoolean() ? 1 : 0);
            } else {
              c.setNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
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
