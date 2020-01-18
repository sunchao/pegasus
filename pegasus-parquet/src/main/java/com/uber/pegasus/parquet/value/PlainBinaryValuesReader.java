package com.uber.pegasus.parquet.value;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

public class PlainBinaryValuesReader extends AbstractPlainValuesReader<VarBinaryVector> {
  private static final int SIZE_OF_INT = Integer.BYTES;

  @Override
  public void read(VarBinaryVector c, int rowId) {
    c.set(rowId, readBytes().getBytes());
  }

  @Override
  public void readBatch(VarBinaryVector c, int rowId, int total) {
    /**
     * For variable length, we need loop through every element because we don't know the length and
     * position of each element before hand.
     */
    int start = rowId;
    for (int i = 0; i < total; i++) {
      ByteBuffer lengthBuf = getBuffer(SIZE_OF_INT);
      int length = lengthBuf.getInt();
      ByteBuffer buffer = getBuffer(length);
      c.set(start++, buffer, buffer.position(), length);
    }
  }

  @Override
  public Binary readBytes() {
    try {
      int length = getBuffer(SIZE_OF_INT).getInt();
      return Binary.fromConstantByteBuffer(getBuffer(length));
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + position(), e);
    }
  }
}
