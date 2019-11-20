package com.uber.pegasus.parquet.value;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.junit.Test;

public class TestRleIntValuesReader {
  private static final ByteBufferAllocator ALLOCATOR = new DirectByteBufferAllocator();
  private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  @Test
  public void testBasic() throws Exception {
    final int BIT_WIDTH = 3;
    final int SIZE = 200;
    RunLengthBitPackingHybridValuesWriter encoder = getEncoder(BIT_WIDTH, 5, 10);

    for (int i = 0; i < 100; i++) {
      encoder.writeInteger(4);
    }

    for (int i = 0; i < 100; i++) {
      encoder.writeInteger(5);
    }

    ByteBuffer bb = encoder.getBytes().toByteBuffer();
    ByteBufferInputStream in = ByteBufferInputStream.wrap(bb);
    RleIntValuesReader decoder = new RleIntValuesReader(BUFFER_ALLOCATOR, BIT_WIDTH);
    decoder.initFromPage(SIZE, in);

    IntVector out = new IntVector("test", BUFFER_ALLOCATOR);
    out.allocateNew(SIZE);
    decoder.readBatch(out, 0, SIZE);

    for (int i = 0; i < 100; i++) {
      assertEquals(4, out.get(i));
    }

    for (int i = 100; i < SIZE; i++) {
      assertEquals(5, out.get(i));
    }
  }

  private RunLengthBitPackingHybridValuesWriter getEncoder(
      int bitWidth, int initialCapacity, int pageSize) {
    return new RunLengthBitPackingHybridValuesWriter(
        bitWidth, initialCapacity, pageSize, ALLOCATOR);
  }
}
