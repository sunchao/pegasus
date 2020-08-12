package com.uber.pegasus.parquet.value;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.Float4Vector;

public class PlainFloatValuesReader extends AbstractPlainValuesReader<Float4Vector> {
  private static final int SIZE_OF_FLOAT = Float.BYTES;

  @Override
  public void read(Float4Vector c, int rowId) {
    c.set(rowId, getBuffer(SIZE_OF_FLOAT).getFloat());
  }

  @Override
  public void readBatch(Float4Vector c, int rowId, int total) {
    int requiredBytes = total * SIZE_OF_FLOAT;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * SIZE_OF_FLOAT, buffer);
  }

  @Override
  public float readFloat() {
    return getBuffer(SIZE_OF_FLOAT).getFloat();
  }
}
