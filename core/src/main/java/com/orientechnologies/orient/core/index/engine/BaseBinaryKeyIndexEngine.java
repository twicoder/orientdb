package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import java.util.stream.Stream;

public interface BaseBinaryKeyIndexEngine extends BaseIndexEngine {
  Stream<ORawPair<byte[], ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer);

  Stream<ORawPair<byte[], ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer);

  Stream<ORawPair<byte[], ORID>> iterateEntriesMinor(
      final Object toKey,
      final boolean isInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer);

  Stream<ORawPair<byte[], ORID>> stream(ValuesTransformer valuesTransformer);

  Stream<ORawPair<byte[], ORID>> descStream(ValuesTransformer valuesTransformer);

  Stream<byte[]> keyStream();

  static Object enhanceToCompositeKeyBetweenAsc(
      final Object keyTo, final boolean toInclusive, final int keySize) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    return enhanceCompositeKey(keyTo, partialSearchModeTo, keySize);
  }

  static Object enhanceFromCompositeKeyBetweenAsc(
      final Object keyFrom, final boolean fromInclusive, final int keySize) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    return enhanceCompositeKey(keyFrom, partialSearchModeFrom, keySize);
  }

  static Object enhanceToCompositeKeyBetweenDesc(
      final Object keyTo, final boolean toInclusive, final int keySize) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    return enhanceCompositeKey(keyTo, partialSearchModeTo, keySize);
  }

  static Object enhanceFromCompositeKeyBetweenDesc(
      final Object keyFrom, final boolean fromInclusive, final int keySize) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    return enhanceCompositeKey(keyFrom, partialSearchModeFrom, keySize);
  }

  static Object enhanceCompositeKeyMajorAsc(
      final Object key, final boolean inclusive, final int keySize) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    return enhanceCompositeKey(key, partialSearchMode, keySize);
  }

  static Object enhanceCompositeKeyMajorDesc(
      Object key, final boolean inclusive, final int keySize) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    return enhanceCompositeKey(key, partialSearchMode, keySize);
  }

  static Object enhanceCompositeKeyMinorDesc(
      final Object key, final boolean inclusive, final int keySize) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    return enhanceCompositeKey(key, partialSearchMode, keySize);
  }

  static Object enhanceCompositeKeyMinorAsc(
      final Object key, final boolean inclusive, final int keySize) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    return enhanceCompositeKey(key, partialSearchMode, keySize);
  }

  static Object enhanceCompositeKey(
      final Object key, final PartialSearchMode partialSearchMode, final int keySize) {
    if (!(key instanceof OCompositeKey)) {
      return key;
    }

    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (!(keySize == 1
        || compositeKey.getKeys().size() == keySize
        || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY)) {
        keyItem = OAlwaysGreaterKey.INSTANCE;
      } else {
        keyItem = OAlwaysLessKey.INSTANCE;
      }

      for (int i = 0; i < itemsToAdd; i++) {
        fullKey.addKey(keyItem);
      }

      return fullKey;
    }

    return key;
  }

  enum PartialSearchMode {
    /** Any partially matched key will be used as search result. */
    NONE,
    /** The biggest partially matched key will be used as search result. */
    HIGHEST_BOUNDARY,

    /** The smallest partially matched key will be used as search result. */
    LOWEST_BOUNDARY
  }
}
