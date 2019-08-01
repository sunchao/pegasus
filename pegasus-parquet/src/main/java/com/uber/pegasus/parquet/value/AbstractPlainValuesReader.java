package com.uber.pegasus.parquet.value;

import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractPlainValuesReader<V extends ValueVector>
    extends ValuesReader<V> {
  private ByteBufferInputStream in;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) {
    this.in = in;
  }

  protected ByteBuffer getBuffer(int length) {
    try {
      return in.slice(length).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }
}
