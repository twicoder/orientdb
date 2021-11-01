package com.orientechnologies.common.comparator;

import java.util.Arrays;

public class ArrayWrapper {
  private final byte[] key;

  @Override
  public String toString() {
    return "ArrayWrapper{key=" + Arrays.toString(key) + '}';
  }

  public ArrayWrapper(byte[] key) {
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrayWrapper that = (ArrayWrapper) o;
    return Arrays.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }
}
