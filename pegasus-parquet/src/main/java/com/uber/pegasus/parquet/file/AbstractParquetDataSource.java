package com.uber.pegasus.parquet.file;

import static java.util.Objects.requireNonNull;

import org.apache.arrow.memory.ArrowBuf;

public abstract class AbstractParquetDataSource implements AutoCloseable {
  private final String path;
  private long readTimeNanos;
  private long readBytes;

  public AbstractParquetDataSource(String path) {
    this.path = requireNonNull(path, "id is null");
  }

  public String getPath() {
    return path;
  }

  public final long getReadBytes() {
    return readBytes;
  }

  public long getReadTimeNanos() {
    return readTimeNanos;
  }

  public final void readFully(long position, byte[] buffer) {
    readFully(position, buffer, 0, buffer.length);
  }

  public final void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength) {
    readBytes += bufferLength;

    long start = System.nanoTime();
    readInternal(position, buffer, bufferOffset, bufferLength);
    long currentReadTimeNanos = System.nanoTime() - start;

    readTimeNanos += currentReadTimeNanos;
  }

  public final void readFully(long position, ArrowBuf arrowBuf, int readChunkSize) {
    readBytes += readChunkSize;

    long start = System.nanoTime();
    readInternal(position, arrowBuf, readChunkSize);
    long currentReadTimeNanos = System.nanoTime() - start;

    readTimeNanos += currentReadTimeNanos;
  }

  protected abstract void readInternal(
      long position, byte[] buffer, int bufferOffset, int bufferLength);

  protected abstract void readInternal(long position, ArrowBuf arrowBuf, int readChunkSize);
}
