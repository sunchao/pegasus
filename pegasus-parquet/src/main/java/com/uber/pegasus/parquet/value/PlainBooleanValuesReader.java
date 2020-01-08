package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.BitVector;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.bitpacking.Packer;

public class PlainBooleanValuesReader extends AbstractPlainValuesReader<BitVector> {
  private ByteBitPackingValuesReader in = new ByteBitPackingValuesReader(1, Packer.LITTLE_ENDIAN);

  @Override
  public void read(BitVector c, int rowId) {
    c.set(rowId, in.readInteger());
  }

  @Override
  public void readBatch(BitVector c, int rowId, int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBoolean() {
    return in.readInteger() == 0 ? false : true;
  }
}
