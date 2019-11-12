package com.uber.pegasus.parquet.value;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.Float8Vector;

import java.nio.ByteBuffer;

public class PlainDoubleValuesReader extends AbstractPlainValuesReader<Float8Vector> {
  @Override
  public void read(Float8Vector c, int rowId) {
    c.set(rowId, getBuffer(8).getDouble());
  }

  @Override
  public void readBatch(Float8Vector c, int rowId, int total) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * 8, buffer);
  }

  @Override
  public double readDouble() {
    return getBuffer(8).getDouble();
  }
}
