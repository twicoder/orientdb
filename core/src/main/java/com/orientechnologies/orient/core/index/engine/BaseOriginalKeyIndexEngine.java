package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.stream.Stream;

public interface BaseOriginalKeyIndexEngine extends BaseIndexEngine {
  Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      BaseIndexEngine.ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      BaseIndexEngine.ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      final Object toKey,
      final boolean isInclusive,
      boolean ascSortOrder,
      BaseIndexEngine.ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> stream(BaseIndexEngine.ValuesTransformer valuesTransformer);

  Stream<ORawPair<Object, ORID>> descStream(BaseIndexEngine.ValuesTransformer valuesTransformer);

  Stream<Object> keyStream();
}
