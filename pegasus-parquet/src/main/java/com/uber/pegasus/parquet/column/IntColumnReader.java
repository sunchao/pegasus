package com.uber.pegasus.parquet.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.OriginalType;

import java.io.IOException;

public class IntColumnReader extends AbstractColumnReader<IntVector> {
  public IntColumnReader(
      ColumnDescriptor desc,
      OriginalType originalType,
      PageReader pageReader,
      BufferAllocator allocator) throws IOException {
    super(desc, originalType, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(IntVector column, IntVector dictionaryIds,
      int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      if (!column.isNull(i)) {
        column.set(i, dictionary.decodeToInt(dictionaryIds.get(i)));
      }
    }
  }
}
