package com.uber.pegasus.parquet.value;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestRleIntValuesReader {

  @Test
  public void testBasic() throws Exception {
    final int SIZE = 200;
    RunLengthBitPackingHybridValuesWriter encoder = getEncoder(3, 5, 10);

    for (int i = 0; i < 100; i++) {
      encoder.writeInteger(4);
    }

    for (int i = 0; i < 100; i++) {
      encoder.writeInteger(5);
    }

    ByteBuffer bb = encoder.getBytes().toByteBuffer();
    ByteBufferInputStream in = ByteBufferInputStream.wrap(bb);
    RleIntValuesReader decoder = new RleIntValuesReader(3);
    decoder.initFromPage(SIZE, in);

    IntVector out = new IntVector("test", new RootAllocator(Long.MAX_VALUE));
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
    return new RunLengthBitPackingHybridValuesWriter(bitWidth, initialCapacity,
        pageSize, new DirectByteBufferAllocator());
  }
}
