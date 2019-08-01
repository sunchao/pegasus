package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.BitVector;

public class PlainBooleanValuesReader extends AbstractPlainValuesReader<BitVector> {
  @Override
  public void readBatch(BitVector c, int rowId, int total) {
    throw new UnsupportedOperationException();
  }
}
