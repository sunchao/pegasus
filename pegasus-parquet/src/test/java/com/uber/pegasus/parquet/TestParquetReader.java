package com.uber.pegasus.parquet;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestParquetReader {

  @Test
  public void testBasic() throws IOException {
    Path path = new Path("./test-files/int32_decimal.parquet");
    FileSystem fileSystem = path.getFileSystem(new Configuration());
    FSDataInputStream inputStream = fileSystem.open(path);
    long fileSize = fileSystem.getFileStatus(path).getLen();
  }

  /*

  public static void createParquetPageSource(
          Configuration configuration,
          Path path,
          long start,
          long length,
          long fileSize,
          Properties schema,
          List<HiveColumnHandle> columns,
          boolean useParquetColumnNames,
          boolean failOnCorruptedParquetStatistics,
          TypeManager typeManager,
          TupleDomain<HiveColumnHandle> effectivePredicate,
          FileFormatDataSourceStats stats)
  {

    try {
      HdfsContext hdfsContext = new HdfsContext(session);
      FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path, configuration);
      FSDataInputStream inputStream = fileSystem.open(path);
      ParquetMetadata parquetMetadata = MetadataReader.readFooter(inputStream, path, fileSize);
      FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
      MessageType fileSchema = fileMetaData.getSchema();
      dataSource = buildHdfsParquetDataSource(inputStream, path, stats);

      Optional<MessageType> optionalRequestedSchema = columns.stream()
              .filter(column -> column.getColumnType() == REGULAR)
              .map(column -> getColumnType(column, fileSchema, useParquetColumnNames))
              .filter(Objects::nonNull)
              .map(type -> new MessageType(fileSchema.getName(), type))
              .reduce(MessageType::union);

      MessageType requestedSchema = optionalRequestedSchema.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

      ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
      for (BlockMetaData block : parquetMetadata.getBlocks()) {
        long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
        if (firstDataPage >= start && firstDataPage < start + length) {
          footerBlocks.add(block);
        }
      }

      Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
      TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
      Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
      final ParquetDataSource finalDataSource = dataSource;
      ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
      for (BlockMetaData block : footerBlocks.build()) {
        if (predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
          blocks.add(block);
        }
      }
      MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
      ParquetReader parquetReader = new ParquetReader(
              messageColumnIO,
              blocks.build(),
              dataSource,
              systemMemoryContext);
    }
    catch (Exception e) {

    }
  } */

}
