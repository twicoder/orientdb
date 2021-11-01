package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Collection;
import java.util.stream.Stream;

public interface IndexInternalOriginalKey extends IndexInternal {
  Stream<ORawPair<Object, ORID>> stream();

  Stream<ORawPair<Object, ORID>> descStream();

  Stream<Object> keyStream();

  /**
   * Returns stream which presents subset of index data between passed in keys.
   *
   * @param fromKey Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param toKey Upper border of index data.
   * @param toInclusive Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return Cursor which presents subset of index data between passed in keys.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder);

  /**
   * Returns stream which presents data associated with passed in keys.
   *
   * @param keys Keys data of which should be returned.
   * @param ascSortOrder Flag which determines whether data iterated by stream should be in
   *     ascending or descending order.
   * @return stream which presents data associated with passed in keys.
   */
  Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is greater than
   * passed in key.
   *
   * @param fromKey Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return stream which presents subset of data which associated with key which is greater than
   *     passed in key.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is less than
   * passed in key.
   *
   * @param toKey Upper border of index data.
   * @param toInclusive Indicates Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder Flag which determines whether data iterated by stream should be in ascending or
   *     descending order.
   * @return stream which presents subset of data which associated with key which is less than
   *     passed in key.
   */
  Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder);
}
