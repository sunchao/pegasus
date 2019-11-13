package com.uber.pegasus.parquet.column;

import com.uber.pegasus.parquet.LevelsReader;
import com.uber.pegasus.parquet.value.PlainBooleanValuesReader;
import com.uber.pegasus.parquet.value.PlainDoubleValuesReader;
import com.uber.pegasus.parquet.value.PlainFloatValuesReader;
import com.uber.pegasus.parquet.value.PlainIntValuesReader;
import com.uber.pegasus.parquet.value.PlainLongValuesReader;
import com.uber.pegasus.parquet.value.RleBooleanValuesReader;
import com.uber.pegasus.parquet.value.RleIntValuesReader;
import com.uber.pegasus.parquet.value.ValuesReader;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.OriginalType;

/**
 * Decoder to return values from a single column. The column vector type is provided via the generic
 * parameter `V` and MUST be consistent with the `ColumnDescriptor` and `OriginalType` passed to the
 * constructor.
 */
public abstract class AbstractColumnReader<V extends ValueVector> {
  private final ColumnDescriptor desc;
  private final OriginalType originalType;
  private final PageReader pageReader;

  /** If true, the current page is dictionary encoded */
  private boolean isCurrentPageDictionaryEncoded;

  /** Total number of values we have read so far (across all pages) */
  private long valuesRead;

  /** The total number of values in the current page. */
  private int pageValueCount;

  /**
   * value that indicates the end of the current page. That is, if valuesRead ==
   * endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /** The total number of values in this column */
  private final long totalValueCount;

  /** Maximum definition level for this column. */
  private final int maxDefLevel;

  /** Decoder for definition levels */
  private LevelsReader<V> defColumn;

  /** The dictionary, if this column has dictionary encoding */
  protected final Dictionary dictionary;

  /** Stores the IDs for dictionary page. */
  private IntVector dictionaryIds;

  /** Decode for values */
  protected ValuesReader<V> valueColumn;

  /** Allocator used to allocate new buffer memory */
  protected final BufferAllocator allocator;

  public AbstractColumnReader(
      ColumnDescriptor desc,
      OriginalType originalType,
      PageReader pageReader,
      BufferAllocator allocator)
      throws IOException {
    this.desc = desc;
    this.originalType = originalType;
    this.pageReader = pageReader;
    this.maxDefLevel = desc.getMaxDefinitionLevel();
    this.allocator = allocator;

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      this.dictionary = dictionaryPage.getEncoding().initDictionary(desc, dictionaryPage);
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }

  public void readBatch(int total, V column) {
    int rowId = 0;
    while (total > 0) {
      // compute the number of values we want to read in this page
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }

      int valuesInCurrentPage = Math.min(total, leftInPage);

      // The current page is dictionary-encoded
      // In this case, the dictionary should already be prepared and we'll need to
      // first decode the dictionary ids, and then dictionary values into the value column
      // TODO: can we lazily decode the dictionary values?
      if (isCurrentPageDictionaryEncoded) {
        if (dictionaryIds == null) {
          dictionaryIds = new IntVector("dictionary", allocator);
        }
        dictionaryIds.reset();
        if (dictionaryIds.getValueCapacity() < total) {
          dictionaryIds.setInitialCapacity(total);
          dictionaryIds.allocateNew();
        }
        defColumn.readBatch(
            dictionaryIds, column, rowId, total, maxDefLevel, (RleIntValuesReader) valueColumn);

        // we'll need to decode the dictionary values - however this depends on the type
        // of column
        decodeDictionary(column, dictionaryIds, rowId, total);
      } else {
        defColumn.readBatch(column, rowId, total, maxDefLevel, valueColumn);
      }

      valuesRead += valuesInCurrentPage;
      total -= valuesInCurrentPage;
      rowId += valuesInCurrentPage;
    }
  }

  protected abstract void decodeDictionary(V column, IntVector dictionaryIds, int rowId, int total);

  private void readPage() {
    DataPage dataPage = pageReader.readPage();
    dataPage.accept(
        new DataPage.Visitor<Void>() {
          @Override
          public Void visit(DataPageV1 dataPageV1) {
            try {
              readPageV1(dataPageV1);
              return null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public Void visit(DataPageV2 dataPageV2) {
            try {
              readPageV2(dataPageV2);
              return null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  @SuppressWarnings("deprecation")
  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in) throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      // a data page for dictionary encoding
      if (dictionary == null) {
        throw new IOException(
            "Could not read page in column "
                + desc
                + " as the dictionary was missing "
                + "for encoding "
                + dataEncoding);
      }
      if (dataEncoding != Encoding.PLAIN_DICTIONARY && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException(
            "Unsupported encoding: " + dataEncoding + " for dictionary data page");
      }
      this.valueColumn = getRleValuesReader(desc, allocator);
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException(
            "Unsupported encoding: " + dataEncoding + " for non-dictionary data page");
      }
      this.valueColumn = getPlainValuesReader(desc);
      this.isCurrentPageDictionaryEncoded = false;
    }

    valueColumn.initFromPage(pageValueCount, in);
  }

  @SuppressWarnings("unchecked")
  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.valuesRead = 0;
    if (page.getDlEncoding() != Encoding.RLE && desc.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException(
          "Unsupported definition level encoding: " + page.getDlEncoding());
    }

    // create def & rep level decoders to init input stream
    int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());

    BytesInput bytes = page.getBytes();
    ByteBufferInputStream in = bytes.toInputStream();

    // only used now to consume the repetition level data
    page.getRlEncoding()
        .getValuesReader(desc, ValuesType.REPETITION_LEVEL)
        .initFromPage(pageValueCount, in);

    defColumn = new LevelsReader<>(allocator, bitWidth);
    defColumn.initFromPage(pageValueCount, in);
    initDataReader(page.getValueEncoding(), in);
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.valuesRead = 0;
    int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    defColumn = new LevelsReader<>(allocator, bitWidth, false);
    defColumn.initFromPage(pageValueCount, page.getDefinitionLevels().toInputStream());
    initDataReader(page.getDataEncoding(), page.getData().toInputStream());
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> ValuesReader<V> getRleValuesReader(
      ColumnDescriptor desc, BufferAllocator allocator) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ValuesReader<V>) new RleBooleanValuesReader(allocator);
      case INT32:
        return (ValuesReader<V>) new RleIntValuesReader(allocator);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for RLE encoding: " + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> ValuesReader<V> getPlainValuesReader(
      ColumnDescriptor desc) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ValuesReader<V>) new PlainBooleanValuesReader();
      case INT32:
        return (ValuesReader<V>) new PlainIntValuesReader();
      case INT64:
        return (ValuesReader<V>) new PlainLongValuesReader();
      case FLOAT:
        return (ValuesReader<V>) new PlainFloatValuesReader();
      case DOUBLE:
        return (ValuesReader<V>) new PlainDoubleValuesReader();
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for plain encoding: "
                + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }
}
