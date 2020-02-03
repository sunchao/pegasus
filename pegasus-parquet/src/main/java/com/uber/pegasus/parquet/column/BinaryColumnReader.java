package com.uber.pegasus.parquet.column;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;

public class BinaryColumnReader extends AbstractColumnReader<VarBinaryVector> {
  public BinaryColumnReader(
      ColumnDescriptor desc,
      OriginalType originalType,
      PageReader pageReader,
      BufferAllocator allocator)
      throws IOException {
    super(desc, originalType, pageReader, allocator);
  }

  @Override
  protected void decodeDictionary(
      VarBinaryVector column, IntVector dictionaryIds, int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      if (!column.isNull(i)) {
        Binary binary = dictionary.decodeToBinary(dictionaryIds.get(i));
        column.set(i, binary.getBytes());
      }
    }
  }
}
