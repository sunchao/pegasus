package com.uber.pegasus.parquet.value;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestPlainValuesReader {
  private static final int SIZE = 100;
  private static final int INITIAL_CAPACITY = 10;
  private static final int PAGE_SIZE = 100;
  private static final ByteBufferAllocator ALLOCATOR = new DirectByteBufferAllocator();
  private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  @Test
  public void testIntBasic() throws IOException {
    int[] expected = new int[SIZE];
    for (int i = 0; i < SIZE; i++) {
      expected[i] = 42;
    }

    PlainValuesWriter writer = getValuesWriter();
    PlainIntValuesReader reader = new PlainIntValuesReader();
    IntVector out = new IntVector("test", BUFFER_ALLOCATOR);
    for (int v : expected) {
      writer.writeInteger(v);
    }

    process(writer, reader, out);

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], out.get(i));
    }
  }

  @Test
  public void testLongBasic() throws IOException {
    long[] expected = new long[SIZE];
    for (int i = 0; i < SIZE; i++) {
      expected[i] = 42;
    }

    PlainValuesWriter writer = getValuesWriter();
    PlainLongValuesReader reader = new PlainLongValuesReader();
    BigIntVector out = new BigIntVector("test", BUFFER_ALLOCATOR);
    for (long v : expected) {
      writer.writeLong(v);
    }

    process(writer, reader, out);

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], out.get(i));
    }
  }

  @Test
  public void testFloatBasic() throws IOException {
    float[] expected = new float[SIZE];
    for (int i = 0; i < SIZE; i++) {
      expected[i] = 3.14f;
    }

    PlainValuesWriter writer = getValuesWriter();
    PlainFloatValuesReader reader = new PlainFloatValuesReader();
    Float4Vector out = new Float4Vector("test", BUFFER_ALLOCATOR);
    for (float v : expected) {
      writer.writeFloat(v);
    }

    process(writer, reader, out);

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], out.get(i), 0.1f);
    }
  }

  @Test
  public void testDoubleBasic() throws IOException {
    double[] expected = new double[SIZE];
    for (int i = 0; i < SIZE; i++) {
      expected[i] = 3.14;
    }

    PlainValuesWriter writer = getValuesWriter();
    PlainDoubleValuesReader reader = new PlainDoubleValuesReader();
    Float8Vector out = new Float8Vector("test", BUFFER_ALLOCATOR);
    for (double v : expected) {
      writer.writeDouble(v);
    }

    process(writer, reader, out);

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], out.get(i), 0.1f);
    }
  }

  @Test
  public void testFixedLenByteArrayBasic() throws IOException {
    Binary[] expected = new Binary[SIZE];
    for (int i = 0; i < SIZE; i++) {
      expected[i] =
          Binary.fromConstantByteArray(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07});
    }

    int length = expected[0].length();
    FixedLenByteArrayPlainValuesWriter writer = getFixedLenByteArrayValuesWriter(length);
    PlainFixedLenByteArrayValuesReader reader = new PlainFixedLenByteArrayValuesReader(length);
    FixedSizeBinaryVector out = new FixedSizeBinaryVector("test", BUFFER_ALLOCATOR, length);
    for (Binary v : expected) {
      writer.writeBytes(v);
    }

    process(writer, reader, out);

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], Binary.fromConstantByteArray(out.get(i)));
    }
  }

  private static PlainValuesWriter getValuesWriter() {
    return new PlainValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, ALLOCATOR);
  }

  private static FixedLenByteArrayPlainValuesWriter getFixedLenByteArrayValuesWriter(int length) {
    return new FixedLenByteArrayPlainValuesWriter(length, INITIAL_CAPACITY, PAGE_SIZE, ALLOCATOR);
  }

  private static <V extends ValueVector> void process(
      ValuesWriter writer, AbstractPlainValuesReader<V> reader, V out) throws IOException {
    ByteBuffer buf = writer.getBytes().toByteBuffer();
    ByteBufferInputStream in = ByteBufferInputStream.wrap(buf);
    reader.initFromPage(SIZE, in);
    out.allocateNew();

    for (int i = 0; i < SIZE; i++) {
      BitVectorHelper.setValidityBit(out.getValidityBuffer(), i, 1);
    }

    reader.readBatch(out, 0, SIZE);
  }
}
