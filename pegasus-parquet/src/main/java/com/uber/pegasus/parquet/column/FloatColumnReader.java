package com.uber.pegasus.parquet.column;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;

public class FloatColumnReader extends AbstractColumnReader<Float4Vector> {
  public FloatColumnReader(ColumnDescriptor desc, PageReader pageReader, BufferAllocator allocator)
      throws IOException {
    super(desc, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(
      Float4Vector column, IntVector dictionaryIds, int rowId, int total) {
    for (int i = 0; i < rowId + total; i++) {
      if (!column.isNull(i)) {
        column.set(i, dictionary.decodeToFloat(dictionaryIds.get(i)));
      }
    }
  }
}
