package com.uber.pegasus.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;

public final class FooterReader {
  private static final int FOOTER_LENGTH = 8;
  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);

  public FooterReader() {}

  public ParquetMetadata readMetadata(FileSystem fs, Path file, long fileSize) {
    // Layout of Parquet file
    // +---------------------------+---+-----+
    // |      Rest of file         | B |  A  |
    // +---------------------------+---+-----+
    // where A: parquet footer, B: parquet metadata.
    //
    if (fileSize < FOOTER_LENGTH) {
      throw new ParquetDecodingException(
          "file size (" + fileSize + ") is smaller than footer size (8)");
    }
    try (FSDataInputStream in = fs.open(file)) {
      byte[] footerBuf = new byte[FOOTER_LENGTH];
      in.readFully(fileSize - FOOTER_LENGTH, footerBuf);
      ByteArrayInputStream footerStream = new ByteArrayInputStream(footerBuf);
      int metadataLength = BitUtil.readIntLittleEndian(footerStream);

      byte[] magic = new byte[4];
      footerStream.read(magic);
      if (!Arrays.equals(MAGIC, magic)) {
        throw new ParquetDecodingException("Magic bytes not found");
      }

      byte[] metadataBuf = new byte[metadataLength];
      in.readFully(fileSize - FOOTER_LENGTH - metadataLength, metadataBuf);
      FileMetaData fileMetaData = Util.readFileMetaData(new ByteArrayInputStream(metadataBuf));
      return new ParquetMetadataConverter().fromParquetMetadata(fileMetaData);

    } catch (IOException e) {
      throw new ParquetDecodingException("Error reading file: " + file, e);
    }
  }
}
