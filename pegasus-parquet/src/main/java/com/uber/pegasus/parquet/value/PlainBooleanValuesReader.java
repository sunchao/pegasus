package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.BitVector;

public class PlainBooleanValuesReader extends AbstractPlainValuesReader<BitVector> {
  @Override
  public void read(BitVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBatch(BitVector c, int rowId, int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException();
  }
}
