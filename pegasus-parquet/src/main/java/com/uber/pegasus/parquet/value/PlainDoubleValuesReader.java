package com.uber.pegasus.parquet.value;

import io.netty.buffer.ArrowBuf;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.Float8Vector;

public class PlainDoubleValuesReader extends AbstractPlainValuesReader<Float8Vector> {
  private static final int SIZE_OF_DOUBLE = Double.BYTES;

  @Override
  public void read(Float8Vector c, int rowId) {
    c.set(rowId, getBuffer(SIZE_OF_DOUBLE).getDouble());
  }

  @Override
  public void readBatch(Float8Vector c, int rowId, int total) {
    int requiredBytes = total * SIZE_OF_DOUBLE;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * SIZE_OF_DOUBLE, buffer);
  }

  @Override
  public double readDouble() {
    return getBuffer(SIZE_OF_DOUBLE).getDouble();
  }
}
