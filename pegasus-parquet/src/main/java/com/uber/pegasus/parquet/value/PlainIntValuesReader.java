package com.uber.pegasus.parquet.value;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.IntVector;

import java.nio.ByteBuffer;

public class PlainIntValuesReader extends AbstractPlainValuesReader<IntVector> {
  @Override
  public void read(IntVector c, int rowId) {
    c.set(rowId, getBuffer(4).getInt());
  }

  @Override
  public void readBatch(IntVector c, int rowId, int total) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * 4, buffer);
  }

  @Override
  public int readInteger() {
    return getBuffer(4).getInt();
  }
}
