package com.uber.pegasus.parquet.file;

import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class ParquetDataSource extends AbstractParquetDataSource {
  private final FSDataInputStream in;

  public ParquetDataSource(FSDataInputStream in, Path path) {
    super(path.toUri().getPath());
    this.in = in;
  }

  @Override
  protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength) {
    try {
      in.seek(position);
      in.readFully(buffer, bufferOffset, bufferLength);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void readInternal(long position, ArrowBuf arrowBuf, int readChunkSize) {
    try {
      in.seek(position);
      if (readChunkSize > 0) {
        byte[] tmp = new byte[readChunkSize];
        in.readFully(tmp);
        PlatformDependent.copyMemory(tmp, 0, arrowBuf.memoryAddress(), readChunkSize);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws Exception {
    in.close();
  }
}
