package com.uber.pegasus.parquet.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestColumnReader {
  private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator();
  private static final int PAGE_SIZE = 2048;
  private static final Random RAND = new Random();

  @Test
  public void testPlainV1NotNull() throws IOException {
    testPlainInt(true, false, Collections.nCopies(100, Optional.of(42)));
 }

  @Test
  public void testPlainV2NotNull() throws IOException {
    testPlainInt(false, false, Collections.nCopies(100, Optional.of(42)));
 }

 @Test
 public void testPlainV1Null() throws IOException {
    List<Optional<Integer>> data = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      data.add(Optional.empty());
    }
    for (int i = 0; i < 50; i++) {
      data.add(Optional.of(42));
    }
    testPlainInt(false, true, data);
 }

  @Test
  public void TestPlainV2Null() throws IOException {
    List<Optional<Integer>> data = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      boolean isNull = RAND.nextBoolean();
      if (isNull) {
        data.add(Optional.empty());
      } else {
        data.add(Optional.of(RAND.nextInt()));
      }
    }
    testPlainInt(true, true, data);
  }

  private void testPlainInt(boolean useV1, boolean hasNull,
      List<Optional<Integer>> data) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test { " + (hasNull ? "optional" : "required") + " int32 foo; }");
    ColumnDescriptor desc = schema.getColumns().get(0);
    MemPageStore pageStore = new MemPageStore(11);
    ColumnWriteStore writeStore;
    if (useV1) {
      writeStore = new ColumnWriteStoreV1(
          pageStore,
          ParquetProperties.builder()
              .withPageSize(PAGE_SIZE)
              .withDictionaryEncoding(false)
              .build()
      );
    } else {
      writeStore = new ColumnWriteStoreV2(
          schema,
          pageStore,
          ParquetProperties.builder()
              .withPageSize(PAGE_SIZE)
              .withDictionaryEncoding(false)
              .build()
      );
    }
    ColumnWriter columnWriter = writeStore.getColumnWriter(desc);

    for (Optional<Integer> datum : data) {
      if (datum.isPresent()) {
        columnWriter.write(datum.get(), 0, 1);
      } else {
        columnWriter.writeNull(0, 0);
      }
      writeStore.endRecord(); // TODO: better than this?
    }
    writeStore.flush();

    IntColumnReader reader = new IntColumnReader(desc, OriginalType.INT_32,
        pageStore.getPageReader(desc), BUFFER_ALLOCATOR);
    IntVector out = new IntVector("test", BUFFER_ALLOCATOR);
    out.allocateNew();
    reader.readBatch(data.size(), out);

    for (int i = 0; i < data.size(); i++) {
      if (data.get(i).isPresent()) {
        assertFalse("value at index " + i + " should not be null (" +
            data.get(i).get() + ")", out.isNull(i));
        assertEquals(data.get(i).get().intValue(), out.get(i));
      }
    }
  }
}
