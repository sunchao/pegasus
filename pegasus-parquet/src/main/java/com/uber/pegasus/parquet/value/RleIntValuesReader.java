package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.IntVector;

public class RleIntValuesReader extends AbstractRleValuesReader<IntVector> {
  public RleIntValuesReader() {
    super();
  }
  public RleIntValuesReader(int bitWidth) {
    super(bitWidth);
  }

  public RleIntValuesReader(int bitWidth, boolean readLength) {
    super(bitWidth, readLength);
  }

  @Override
  public int readInteger() {
    return readNextInt();
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

  @Override
  public void readBatch(IntVector c, int rowId, int total, int maxDefLevel,
      ValuesReader<IntVector> data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            data.readBatch(c, rowId, total);
          } else {
            for (int i = 0; i < n; i++) {
              c.setNull(rowId + i);
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              c.set(rowId + i, data.readInteger());
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
}
