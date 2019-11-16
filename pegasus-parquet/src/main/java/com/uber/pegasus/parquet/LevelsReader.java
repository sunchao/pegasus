package com.uber.pegasus.parquet;

import com.uber.pegasus.parquet.value.RleIntValuesReader;
import com.uber.pegasus.parquet.value.ValuesReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Class for Parquet level reader.
 * Currently the only implementation for this is RLE-based level reader.
 */
public class LevelsReader<V extends ValueVector> extends RleIntValuesReader {
  public LevelsReader(BufferAllocator allocator, int bitWidth) {
    super(allocator, bitWidth);
  }

  public LevelsReader(BufferAllocator allocator, int bitWidth, boolean readLength) {
    super(allocator, bitWidth, readLength);
  }

  /**
   * Read a batch of `total` values into value vector `c`, starting from offset `rowId`.
   * The values are read from `data`.
   *
   * @param c the value vector to fill in
   * @param rowId the starting offset in value vector to append new values
   * @param total the total number of values to read
   * @param maxDefLevel the maximum definition level for this column
   * @param data the data value reader
   */
  public void readBatch(V c, int rowId, int total, int maxDefLevel,
      ValuesReader<V> data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            data.readBatch(c, rowId, n);
            // TODO: this is not good - we should use something like memset for
            //   multi-byte set bit.
            for (int i = 0; i < n; i++) {
              BitVectorHelper.setValidityBitToOne(c.getValidityBuffer(), rowId + i);
            }
          } else {
            for (int i = 0; i < n; i++) {
              BitVectorHelper.setValidityBit(c.getValidityBuffer(), rowId + i, 0);
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              data.read(c, rowId + i);
              BitVectorHelper.setValidityBitToOne(c.getValidityBuffer(), rowId + i);
            } else {
              BitVectorHelper.setValidityBit(c.getValidityBuffer(), rowId + i, 0);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  /**
   * Read a batch of dictionary IDs from RLE-encoded `data`. The batch size is specified
   * via `total` and starting position is `rowId`. The IDs will be read into `ids` and
   * nulls will be filled in `nulls` which is the vector for the actual values.
   *
   * @param ids the vector where the dictionary IDs will be read into
   * @param nulls the value vector to fill nulls in
   * @param rowId the starting offset in value vector to append new values
   * @param total the total number of values to read
   * @param maxDefLevel the4 maximum definition level for this column
   * @param data the data value reader
   */
  public void readBatch(IntVector ids, V nulls, int rowId, int total, int maxDefLevel,
      RleIntValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            data.readBatch(ids, rowId, n);
            for (int i = 0; i < n; i++) {
              BitVectorHelper.setValidityBitToOne(nulls.getValidityBuffer(), rowId + i);
            }
          } else {
            // TODO: batch this
            for (int i = 0; i < n; i++) {
              BitVectorHelper.setValidityBit(nulls.getValidityBuffer(), rowId + i, 0);
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              ids.set(rowId + i, data.readInteger());
              BitVectorHelper.setValidityBitToOne(nulls.getValidityBuffer(), rowId + i);
            } else {
              BitVectorHelper.setValidityBit(nulls.getValidityBuffer(), rowId, 0);
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
