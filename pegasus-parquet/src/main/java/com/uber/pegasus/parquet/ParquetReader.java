package com.uber.pegasus.parquet;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import com.uber.pegasus.parquet.column.AbstractColumnReader;
import com.uber.pegasus.parquet.column.ColumnReaderFactory;
import com.uber.pegasus.parquet.file.AbstractParquetDataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

public class ParquetReader implements AutoCloseable {
  private static final int MAX_VECTOR_LENGTH = 1024;

  protected final AbstractColumnReader[] columnReaders;
  private final BufferAllocator bufferAllocator;
  private final List<BlockMetaData> blocks;
  private final List<PrimitiveColumnIO> columns;
  private final AbstractParquetDataSource dataSource;
  private int nextRowGroupIdx;
  private long currentRowGroupValueCount;
  private long currentPosInRowGroup;
  private int batchSize;
  private BlockMetaData currentRowGroupMetadata;

  public ParquetReader(
      MessageColumnIO messageColumnIO,
      List<BlockMetaData> blocks,
      AbstractParquetDataSource dataSource,
      BufferAllocator bufferAllocator) {
    this.bufferAllocator = requireNonNull(bufferAllocator, "bufferAllocator is null");
    this.blocks = requireNonNull(blocks, "blocks is null");
    this.dataSource = requireNonNull(dataSource, "dataSource is null");
    columns = requireNonNull(messageColumnIO.getLeaves(), "messageColumnIO is null");
    columnReaders = new AbstractColumnReader[columns.size()];
  }

  public boolean hasNext() {
    if (currentPosInRowGroup >= currentRowGroupValueCount && !advanceToNextRowGroup()) {
      return false;
    }

    batchSize =
        toIntExact(min(MAX_VECTOR_LENGTH, currentRowGroupValueCount - currentPosInRowGroup));

    return true;
  }

  @SuppressWarnings("unchecked")
  public VectorSchemaRoot readNext() throws IOException {
    List<FieldVector> output = new ArrayList<>();

    for (int columnIdx = 0; columnIdx < columns.size(); columnIdx++) {
      ColumnDescriptor columnDescriptor = columns.get(columnIdx).getColumnDescriptor();
      AbstractColumnReader columnReader = columnReaders[columnIdx];
      if (columnReader == null) {
        ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
        PegasusPageReader pageReader = new PegasusPageReader(metadata, dataSource, bufferAllocator);
        columnReader =
            ColumnReaderFactory.createColumnReader(columnDescriptor, pageReader, bufferAllocator);
        columnReaders[columnIdx] = columnReader;
      }

      FieldVector v = ColumnReaderFactory.createFieldVector(columnDescriptor, bufferAllocator);
      v.setInitialCapacity(batchSize);
      v.allocateNew();
      columnReader.readBatch(batchSize, v);

      output.add(v);
    }

    currentPosInRowGroup += batchSize;
    return new VectorSchemaRoot(output);
  }

  @Override
  public void close() throws Exception {
    dataSource.close();
  }

  private boolean advanceToNextRowGroup() {
    if (nextRowGroupIdx == blocks.size()) {
      return false;
    }
    currentRowGroupMetadata = blocks.get(nextRowGroupIdx);
    nextRowGroupIdx++;

    currentPosInRowGroup = 0L;
    currentRowGroupValueCount = currentRowGroupMetadata.getRowCount();

    // reset column readers
    for (int i = 0; i < columns.size(); i++) {
      columnReaders[i] = null;
    }

    return true;
  }

  @SuppressWarnings("deprecation")
  private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor) {
    for (ColumnChunkMetaData metadata : currentRowGroupMetadata.getColumns()) {
      if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
        return metadata;
      }
    }
    throw new IllegalStateException("Metadata is missing for column: " + columnDescriptor);
  }
}
