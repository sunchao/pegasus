package com.uber.pegasus.parquet.column;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.OriginalType;

import java.io.IOException;

public class BooleanColumnReader extends AbstractColumnReader<BitVector> {
  public BooleanColumnReader(ColumnDescriptor desc, OriginalType originalType,
      PageReader pageReader) throws IOException {
    super(desc, originalType, pageReader);
  }

  @Override
  protected void decodeDictionary(BitVector column, IntVector dictionaryIds,
      int rowId, int total) {
    for (int i = rowId; i < rowId + total; i++) {
      column.set(i, dictionary.decodeToBoolean(dictionaryIds.get(i)) ? 1 : 0);
    }
  }
}
