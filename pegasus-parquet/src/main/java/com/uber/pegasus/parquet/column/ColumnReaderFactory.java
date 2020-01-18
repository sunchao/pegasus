package com.uber.pegasus.parquet.column;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;

public class ColumnReaderFactory {
  private ColumnReaderFactory() {}

  public static AbstractColumnReader<?> createColumnReader(
      ColumnDescriptor columnDescriptor, PageReader pageReader, BufferAllocator allocator)
      throws IOException {
    switch (columnDescriptor.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return new BooleanColumnReader(columnDescriptor, pageReader, allocator);
      case INT32:
        return new IntColumnReader(columnDescriptor, pageReader, allocator);
      case INT64:
        return new LongColumnReader(columnDescriptor, pageReader, allocator);
      case FLOAT:
        return new FloatColumnReader(columnDescriptor, pageReader, allocator);
      case DOUBLE:
        return new DoubleColumnReader(columnDescriptor, pageReader, allocator);
      case BINARY:
        return new BinaryColumnReader(columnDescriptor, pageReader, allocator);
    }

    throw new UnsupportedOperationException(
        String.format("Type: %s not yet supported", columnDescriptor));
  }

  public static FieldVector createFieldVector(
      ColumnDescriptor columnDescriptor, BufferAllocator allocator) {
    String columnName = columnDescriptor.getPath()[columnDescriptor.getPath().length - 1];
    switch (columnDescriptor.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return new BitVector(columnName, allocator);
      case INT32:
        return new IntVector(columnName, allocator);
      case INT64:
        return new BigIntVector(columnName, allocator);
      case FLOAT:
        return new Float4Vector(columnName, allocator);
      case DOUBLE:
        return new Float8Vector(columnName, allocator);
      case BINARY:
        return new VarBinaryVector(columnName, allocator);
    }

    throw new UnsupportedOperationException(
        String.format("Type: %s not yet supported", columnDescriptor));
  }
}
