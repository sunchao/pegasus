package com.uber.pegasus.parquet;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import com.uber.pegasus.parquet.column.AbstractColumnReader;
import com.uber.pegasus.parquet.column.ColumnReaderFactory;
import com.uber.pegasus.parquet.file.AbstractParquetDataSource;
import com.uber.pegasus.parquet.file.ParquetDataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReader implements AutoCloseable {
  private static final int MAX_VECTOR_LENGTH = 1024;
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReader.class);

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

  public ParquetReader(FileSystem fs, Path file, int fileSize) throws IOException {
    ParquetMetadata metadata = new FooterReader().readMetadata(fs, file, fileSize);
    this.bufferAllocator = new RootAllocator();
    this.blocks = metadata.getBlocks();
    this.dataSource = new ParquetDataSource(fs.open(file), file);
    ColumnIOFactory factory = new ColumnIOFactory();
    MessageType schema = metadata.getFileMetaData().getSchema();
    LOG.info("Schema = {}", schema);
    this.columns = factory.getColumnIO(schema).getLeaves();
    this.columnReaders = new AbstractColumnReader[columns.size()];
  }

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
      LOG.info("Reading column of type {}", columnDescriptor.getPrimitiveType());
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

    List<Field> fields = output.stream().map(FieldVector::getField).collect(Collectors.toList());

    return new VectorSchemaRoot(fields, output, output.get(0).getValueCount());
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
