package com.uber.pegasus.parquet.value;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;

public class PlainLongValuesReader extends AbstractPlainValuesReader<BigIntVector> {
  private static final int SIZE_OF_LONG = Long.BYTES;

  @Override
  public void read(BigIntVector c, int rowId) {
    c.set(rowId, getBuffer(SIZE_OF_LONG).getLong());
  }

  @Override
  public void readBatch(BigIntVector c, int rowId, int total) {
    int requiredBytes = total * SIZE_OF_LONG;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * SIZE_OF_LONG, buffer);
  }

  @Override
  public long readLong() {
    return getBuffer(SIZE_OF_LONG).getLong();
  }
}
