package com.uber.pegasus.parquet.column;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;

public class IntColumnReader extends AbstractColumnReader<IntVector> {
  public IntColumnReader(ColumnDescriptor desc, PageReader pageReader, BufferAllocator allocator)
      throws IOException {
    super(desc, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(IntVector column, IntVector dictionaryIds, int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      if (!column.isNull(i)) {
        column.set(i, dictionary.decodeToInt(dictionaryIds.get(i)));
      }
    }
  }
}
