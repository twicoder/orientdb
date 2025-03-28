package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

class PureTxBetweenIndexForwardSpliterator implements Spliterator<ORawPair<Object, ORID>> {
  /** */
  private final OIndexOneValue oIndexTxAwareOneValue;

  private final OTransactionIndexChanges indexChanges;
  private Object lastKey;

  private Object nextKey;

  PureTxBetweenIndexForwardSpliterator(
      OIndexOneValue oIndexTxAwareOneValue,
      Object fromKey,
      boolean fromInclusive,
      Object toKey,
      boolean toInclusive,
      OTransactionIndexChanges indexChanges) {
    this.oIndexTxAwareOneValue = oIndexTxAwareOneValue;
    this.indexChanges = indexChanges;

    if (fromKey != null) {
      fromKey =
          this.oIndexTxAwareOneValue.enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
    }
    if (toKey != null) {
      toKey = this.oIndexTxAwareOneValue.enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);
    }

    final Object[] keys = indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
    if (keys.length == 0) {
      nextKey = null;
    } else {
      Object firstKey = keys[0];
      lastKey = keys[1];

      nextKey = firstKey;
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    if (nextKey == null) {
      return false;
    }

    ORawPair<Object, ORID> result;

    do {
      result = this.oIndexTxAwareOneValue.calculateTxIndexEntry(nextKey, null, indexChanges);
      nextKey = indexChanges.getHigherKey(nextKey);

      if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0) {
        nextKey = null;
      }

    } while (result == null && nextKey != null);

    if (result == null) {
      return false;
    }

    action.accept(result);
    return true;
  }

  @Override
  public Spliterator<ORawPair<Object, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return NONNULL | SORTED | ORDERED;
  }

  @Override
  public Comparator<? super ORawPair<Object, ORID>> getComparator() {
    return (entryOne, entryTwo) ->
        ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
