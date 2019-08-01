package com.uber.pegasus.parquet;

import com.uber.pegasus.parquet.value.ValuesReader;
import org.apache.arrow.vector.ValueVector;

public interface LevelsReader<V extends ValueVector> {
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
  void readBatch(V c, int rowId, int total, int maxDefLevel, ValuesReader<V> data);
}
