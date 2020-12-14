package com.github.maqroll;

public enum ChecksumType {
  NONE(0),
  CRC32(4);

  private final int value;

  ChecksumType(int value) {
    this.value = value;
  }

  public static ChecksumType lookup(int value) {
    for (ChecksumType checksumType : values()) {
      if (checksumType.value == value) {
        return checksumType;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }
}
