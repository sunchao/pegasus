package com.uber.pegasus.parquet.column;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;

public class BooleanColumnReader extends AbstractColumnReader<BitVector> {
  public BooleanColumnReader(
      ColumnDescriptor desc, PageReader pageReader, BufferAllocator allocator) throws IOException {
    super(desc, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(BitVector column, IntVector dictionaryIds, int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      column.set(i, dictionary.decodeToBoolean(dictionaryIds.get(i)) ? 1 : 0);
    }
  }
}
