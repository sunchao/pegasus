package com.uber.pegasus.parquet;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import com.uber.pegasus.parquet.file.AbstractParquetDataSource;
import io.netty.buffer.ArrowBuf;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

public class PegasusPageReader implements PageReader {
  private final ColumnChunkMetaData metadata;

  private final ArrowBuf arrowBuf;
  private final int bufferLimit;

  private boolean dictionaryConsumed;
  private int bufferOffset;

  public PegasusPageReader(
      ColumnChunkMetaData metadata,
      AbstractParquetDataSource dataSource,
      BufferAllocator bufferAllocator) {
    this.metadata = requireNonNull(metadata, "columnChunkMetaData is null");
    this.bufferLimit = toIntExact(metadata.getTotalSize());
    this.arrowBuf =
        requireNonNull(bufferAllocator, "bufferAllocator is null").buffer(this.bufferLimit);
    requireNonNull(dataSource, "dataSource is null")
        .readFully(metadata.getStartingPos(), this.arrowBuf, this.bufferLimit);
  }

  @Override
  public DictionaryPage readDictionaryPage() {
    if (!hasDictionary()) {
      return null;
    }

    checkState(!dictionaryConsumed, "dictionary page is already consumed");

    PageHeader pageHeader = readPageHeader();
    checkState(PageType.DICTIONARY_PAGE == pageHeader.getType());

    this.dictionaryConsumed = true;

    return readDictionaryPage(pageHeader);
  }

  @Override
  public long getTotalValueCount() {
    return metadata.getValueCount();
  }

  @Override
  public DataPage readPage() {
    if (bufferOffset >= bufferLimit) {
      return null;
    }

    final PageHeader pageHeader = readPageHeader();
    switch (pageHeader.type) {
      case DICTIONARY_PAGE:
        throw new IllegalStateException(
            metadata + " has more than one dictionary page in column chunk");
      case DATA_PAGE:
        return readDataPageV1(pageHeader);
      case DATA_PAGE_V2:
        return readDataPageV2(pageHeader);
      default:
        skip(pageHeader.getCompressed_page_size());
        break;
    }

    return null;
  }

  private PageHeader readPageHeader() {
    ArrowBufInputStream arrowBufInputStream =
        new ArrowBufInputStream(arrowBuf, bufferOffset, bufferLimit);
    try {
      return Util.readPageHeader(arrowBufInputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      bufferOffset = arrowBufInputStream.getOffset();
    }
  }

  private BytesInput getSlice(int size) {
    BytesInput bytesInput = BytesInput.from(arrowBuf.nioBuffer(bufferOffset, size));
    bufferOffset += size;
    return bytesInput;
  }

  private void skip(int size) {
    this.bufferOffset += size;
  }

  private boolean hasDictionary() {
    long dictionaryPageOffset = metadata.getDictionaryPageOffset();
    long firstDataPageOffset = metadata.getFirstDataPageOffset();
    return dictionaryPageOffset > 0L && dictionaryPageOffset < firstDataPageOffset;
  }

  private DictionaryPage readDictionaryPage(PageHeader pageHeader) {
    DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();
    return new DictionaryPage(
        getSlice(pageHeader.getCompressed_page_size()),
        pageHeader.getUncompressed_page_size(),
        dictHeader.getNum_values(),
        Encoding.valueOf(dictHeader.getEncoding().name()));
  }

  private DataPage readDataPageV1(PageHeader pageHeader) {
    DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
    return new DataPageV1(
        getSlice(pageHeader.getCompressed_page_size()),
        dataHeaderV1.getNum_values(),
        pageHeader.getUncompressed_page_size(),
        null, // TODO: convert thrift to pojo type: dataHeaderV1.getStatistics(),
        Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name()),
        Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name()),
        Encoding.valueOf(dataHeaderV1.getEncoding().name()));
  }

  private DataPage readDataPageV2(PageHeader pageHeader) {
    DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
    int dataSize =
        pageHeader.getCompressed_page_size()
            - dataHeaderV2.getRepetition_levels_byte_length()
            - dataHeaderV2.getDefinition_levels_byte_length();
    return new DataPageV2(
        dataHeaderV2.getNum_rows(),
        dataHeaderV2.getNum_nulls(),
        dataHeaderV2.getNum_values(),
        getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
        getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
        Encoding.valueOf(dataHeaderV2.getEncoding().name()),
        getSlice(dataSize),
        pageHeader.getUncompressed_page_size(),
        null, // TODO: convert thrift to pojo type: dataHeaderV2.getStatistics(),
        dataHeaderV2.isIs_compressed());
  }

  private static class ArrowBufInputStream extends InputStream {
    private final ArrowBuf arrowBuf;

    private int bufferOffset;
    private int bufferLimit;

    public ArrowBufInputStream(ArrowBuf arrowBuf, int bufferOffset, int bufferLimit) {
      this.arrowBuf = requireNonNull(arrowBuf, "arrowBuf is null");
      this.bufferOffset = bufferOffset;
      this.bufferLimit = bufferLimit;
    }

    @Override
    public int read(byte[] b) {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) {
      int readChunkSize = Math.min(len, available());

      if (readChunkSize == 0) {
        return -1;
      }

      arrowBuf.getBytes(bufferOffset, b, off, len);
      bufferOffset += readChunkSize;
      return readChunkSize;
    }

    @Override
    public long skip(long n) {
      long skipChunkSize = Math.min(n, available());

      if (skipChunkSize == 0) {
        return 0;
      }

      bufferOffset += skipChunkSize;

      return skipChunkSize;
    }

    @Override
    public int available() {
      return bufferLimit - bufferOffset;
    }

    @Override
    public synchronized void mark(int readlimit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read() {
      return arrowBuf.getByte(bufferOffset++);
    }

    public int getOffset() {
      return bufferOffset;
    }
  }
}
