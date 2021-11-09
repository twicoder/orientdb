package com.orientechnologies.orient.core.index;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Stream;

public interface IndexInternalBinaryKey extends IndexInternal {
  Stream<ORawPair<byte[], ORID>> stream();

  Stream<ORawPair<byte[], ORID>> descStream();

  Stream<byte[]> keyStream();

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
  Stream<ORawPair<byte[], ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder);

  /**
   * Returns stream which presents data associated with passed in keys.
   *
   * @param keys Keys data of which should be returned.
   * @param ascSortOrder Flag which determines whether data iterated by stream should be in
   *     ascending or descending order.
   * @return stream which presents data associated with passed in keys.
   */
  Stream<ORawPair<byte[], ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder);

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
  Stream<ORawPair<byte[], ORID>> streamEntriesMajor(
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
  Stream<ORawPair<byte[], ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder);

  Collator getCollator();

  KeyNormalizers getKeyNormalizers();

  static ORawPair<Collator, KeyNormalizers> createCollatorNormalizers(
      OStorage storage, ODocument metadata) {
    final Locale locale;
    int decomposition = Collator.NO_DECOMPOSITION;

    if (metadata != null) {
      final String languageTag = metadata.getProperty(ODefaultIndexFactory.BINARY_TREE_LOCALE);
      if (languageTag == null) {
        locale = storage.getConfiguration().getLocaleInstance();
      } else {
        locale = Locale.forLanguageTag(languageTag);
      }

      Object decompositionTagValue =
          metadata.getProperty(ODefaultIndexFactory.BINARY_TREE_DECOMPOSITION);
      String decompositionTag;

      if (decompositionTagValue == null) {
        decompositionTag = null;
      } else {
        decompositionTag = decompositionTagValue.toString();
      }

      if (decompositionTag != null) {
        try {
          decomposition = Integer.parseInt(decompositionTag);
        } catch (NumberFormatException e) {
          // ignore
        }
      }
    } else {
      locale = storage.getConfiguration().getLocaleInstance();
    }

    final Collator collator = Collator.getInstance(locale);
    collator.setDecomposition(decomposition);
    collator.freeze();

    final KeyNormalizers keyNormalizers = new KeyNormalizers(collator);

    return new ORawPair<>(collator, keyNormalizers);
  }
}
