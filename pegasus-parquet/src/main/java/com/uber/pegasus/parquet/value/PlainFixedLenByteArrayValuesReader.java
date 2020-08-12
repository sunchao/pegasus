package com.uber.pegasus.parquet.value;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

public class PlainFixedLenByteArrayValuesReader
    extends AbstractPlainValuesReader<FixedSizeBinaryVector> {
  private int length;

  public PlainFixedLenByteArrayValuesReader(int length) {
    this.length = length;
  }

  @Override
  public void read(FixedSizeBinaryVector c, int rowId) {
    c.set(rowId, readBytes().getBytes(), 0, length);
  }

  @Override
  public void readBatch(FixedSizeBinaryVector c, int rowId, int total) {
    int requiredBytes = total * length;
    ByteBuffer buffer = getBuffer(requiredBytes);
    ArrowBuf valueBuf = c.getDataBuffer();
    valueBuf.setBytes(rowId * length, buffer);
  }

  @Override
  public Binary readBytes() {
    try {
      return Binary.fromConstantByteBuffer(getBuffer(length));
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + position(), e);
    }
  }
}
