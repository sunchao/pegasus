package com.uber.pegasus.parquet.file;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;

public class FileParquetDataSource extends AbstractParquetDataSource {
  private final RandomAccessFile input;

  public FileParquetDataSource(File file) throws FileNotFoundException {
    super(file.getAbsolutePath());
    input = new RandomAccessFile(file, "r");
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength) {
    try {
      input.seek(position);
      input.readFully(buffer, bufferOffset, bufferLength);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void readInternal(long position, ArrowBuf arrowBuf, int readChunkSize) {
    try {
      input.seek(position);
      if (readChunkSize > 0) {
        byte[] tmp = new byte[readChunkSize];
        input.readFully(tmp);
        PlatformDependent.copyMemory(tmp, 0, arrowBuf.memoryAddress(), readChunkSize);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
