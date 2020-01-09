package com.uber.pegasus.parquet.value;

import java.io.IOException;
import org.apache.arrow.vector.BitVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.bitpacking.Packer;

public class PlainBooleanValuesReader extends AbstractPlainValuesReader<BitVector> {
  private ByteBitPackingValuesReader in = new ByteBitPackingValuesReader(1, Packer.LITTLE_ENDIAN);

  @Override
  public void read(BitVector c, int rowId) {
    c.set(rowId, in.readInteger());
    c.setValueCount(1);
  }

  @Override
  public void readBatch(BitVector c, int rowId, int total) {
    int start = rowId;
    for (int i = 0; i < total; i++) {
      c.set(start++, in.readInteger());
    }
    c.setValueCount(total);
  }

  @Override
  public boolean readBoolean() {
    return in.readInteger() == 0 ? false : true;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    this.in.initFromPage(valueCount, stream);
  }
}
