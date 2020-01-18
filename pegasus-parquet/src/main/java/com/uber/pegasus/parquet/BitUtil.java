package com.uber.pegasus.parquet;

import java.io.IOException;
import java.io.InputStream;

/** Utility functions for manipulating bits */
public class BitUtil {
  /** Reads the next varint encoded int */
  public static int readUnsignedVarInt(InputStream in) throws IOException {
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

  /** Reads the next 4 byte little endian int */
  public static int readIntLittleEndian(InputStream in) throws IOException {
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /** Reads the next `byteWidth` little endian int */
  public static int readIntLittleEndianPaddedOnBitWidth(InputStream in, int bytesWidth)
      throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in.read();
      case 2:
        {
          int ch2 = in.read();
          int ch1 = in.read();
          return (ch1 << 8) + ch2;
        }
      case 3:
        {
          int ch3 = in.read();
          int ch2 = in.read();
          int ch1 = in.read();
          return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
        }
      case 4:
        {
          return readIntLittleEndian(in);
        }
    }
    throw new RuntimeException("Unreachable");
  }
}
