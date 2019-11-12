package com.uber.pegasus.parquet.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractRleValuesReader<V extends ValueVector>
    extends ValuesReader<V> {
  /**
   * Encoding mode
   */
  protected enum Mode {
    RLE,
    PACKED
  }

  private ByteBufferInputStream in;
  protected final BufferAllocator allocator;

  // bit/byte width of decoded data and utility to batch unpack them
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // current decoding mode and values
  protected Mode mode;
  protected int currentCount;
  protected int currentValue;

  // buffer of decoded values if the values are PACKED.
  protected int[] currentBuffer = new int[16];
  protected int currentBufferIdx = 0;

  // if true, the bit width is fixed
  // this decoder is used in different places and this also controls if we need to read
  // the bit-width from the beginning of the data stream
  private final boolean fixedWidth;
  private final boolean readLength;

  public AbstractRleValuesReader(BufferAllocator allocator) {
    this.fixedWidth = false;
    this.readLength = false;
    this.allocator = allocator;
  }

  public AbstractRleValuesReader(BufferAllocator allocator, int bitWidth) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    this.allocator = allocator;
    init(bitWidth);
  }

  public AbstractRleValuesReader(BufferAllocator allocator, int bitWidth,
      boolean readLength) {
    this.fixedWidth = true;
    this.readLength = readLength;
    this.allocator = allocator;
    init(bitWidth);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.in = in.sliceStream(length);
      }
    } else {
      // initialize for values
      if (in.available() > 0) {
        init(in.read());
      }
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = Mode.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  /**
   * Put a single value into vector `v` at offset `idx`.
   *
   * @param v the value vector to fill value in
   * @param idx the offset to put the value in the vector
   */
  // abstract protected void setValue(V v, int idx);

  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32,
        "bitWidth must be >= 0 and <= 32, but found " + bitWidth);
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }

  protected int readNextInt() {
    if (this.currentCount == 0) {
      this.readNextGroup();
    }
    this.currentCount--;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.currentBuffer[currentBufferIdx++];
      default:
        throw new RuntimeException("Unreachable");
    }
  }

  /**
   * Reads the next group.
   */
  protected void readNextGroup() {
    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? Mode.RLE : Mode.PACKED;
      switch (mode) {
        case RLE:
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
          return;
        case PACKED:
          int numGroups = header >>> 1;
          this.currentCount = numGroups * 8;

          if (this.currentBuffer.length < this.currentCount) {
            this.currentBuffer = new int[this.currentCount];
          }
          currentBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = in.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.currentBuffer,
                valueIndex);
            valueIndex += 8;
          }
          return;
        default:
          throw new ParquetDecodingException("not a valid mode " + this.mode);
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in.read();
      case 2: {
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

}
