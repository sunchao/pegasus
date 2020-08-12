package com.uber.pegasus.parquet.value;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;

public class PlainIntValuesReader extends AbstractPlainValuesReader<IntVector> {
  private static final int SIZE_OF_INT = Integer.BYTES;

  @Override
  public void read(IntVector c, int rowId) {
    c.set(rowId, getBuffer(SIZE_OF_INT).getInt());
  }

  @Override
  public void readBatch(IntVector c, int rowId, int total) {
    int requiredBytes = total * SIZE_OF_INT;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * SIZE_OF_INT, buffer);
  }

  @Override
  public int readInteger() {
    return getBuffer(SIZE_OF_INT).getInt();
  }
}
