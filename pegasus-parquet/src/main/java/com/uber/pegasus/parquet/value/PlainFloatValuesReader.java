package com.uber.pegasus.parquet.value;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.Float4Vector;

import java.nio.ByteBuffer;

public class PlainFloatValuesReader extends AbstractPlainValuesReader<Float4Vector> {
  @Override
  public void readBatch(Float4Vector c, int rowId, int total) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * 4, buffer);
  }
}
