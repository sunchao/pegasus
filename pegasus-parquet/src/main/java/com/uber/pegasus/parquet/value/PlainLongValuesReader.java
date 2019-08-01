package com.uber.pegasus.parquet.value;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;

import java.nio.ByteBuffer;

public class PlainLongValuesReader extends AbstractPlainValuesReader<BigIntVector> {
  @Override
  public void readBatch(BigIntVector c, int rowId, int total) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * 8, buffer);
  }
}
