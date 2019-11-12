package com.uber.pegasus.parquet.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.OriginalType;

import java.io.IOException;

public class DoubleColumnReader extends AbstractColumnReader<Float8Vector> {
  public DoubleColumnReader(
      ColumnDescriptor desc,
      OriginalType originalType,
      PageReader pageReader,
      BufferAllocator allocator) throws IOException {
    super(desc, originalType, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(Float8Vector column, IntVector dictionaryIds,
      int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      if (!column.isNull(i)) {
        column.set(i, dictionary.decodeToDouble(dictionaryIds.get(i)));
      }
    }
  }
}
