package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.ValueVector;

public abstract class ValuesReader<V extends ValueVector>
    extends org.apache.parquet.column.values.ValuesReader {

  /**
   * Read a single value and put in vector `c` at offset `rowId`.
   *
   * @param c the value vector to put the value to
   * @param rowId the offset in `c`
   */
  public abstract void read(V c, int rowId);

  /**
   * Read a batch of values with size `total` and append to vector `c` starting from offset `rowId`.
   * Note that this doesn't handle null - it assumes all values are not-null and append them into a
   * contiguous region in `c`.
   *
   * @param c the value vector to fill in
   * @param rowId the starting row index in `c`
   * @param total the total number of values to read in this batch
   */
  public abstract void readBatch(V c, int rowId, int total);

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }
}
