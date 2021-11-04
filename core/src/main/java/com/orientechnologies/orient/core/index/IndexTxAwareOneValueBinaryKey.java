package com.orientechnologies.orient.core.index;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.CollatorComparator;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexTxAwareOneValueBinaryKey extends OIndexTxAware<OIdentifiable>
    implements IndexInternalBinaryKey {

  private final Collator collator;
  private final KeyNormalizers keyNormalizers;
  private final OType[] keyTypes;
  private final CollatorComparator comparator;

  public IndexTxAwareOneValueBinaryKey(
      final ODatabaseDocumentInternal database,
      final IndexInternal delegate,
      Collator collator,
      final KeyNormalizers keyNormalizers,
      final OType[] keyTypes) {
    super(database, delegate);

    this.collator = collator;

    this.keyNormalizers = keyNormalizers;
    this.keyTypes = keyTypes;
    this.comparator = new CollatorComparator(collator);
  }

  private class PureTxBetweenIndexForwardSpliterator
      implements Spliterator<ORawPair<byte[], ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private Object lastKey;

    private Object nextKey;

    private PureTxBetweenIndexForwardSpliterator(
        Object fromKey,
        boolean fromInclusive,
        Object toKey,
        boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      if (fromKey != null) {
        fromKey = enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
      }
      if (toKey != null) {
        toKey = enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);
      }

      final Object[] keys =
          indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
      if (keys.length == 0) {
        nextKey = null;
      } else {
        Object firstKey = keys[0];
        lastKey = keys[1];

        nextKey = firstKey;
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<byte[], ORID>> action) {
      if (nextKey == null) {
        return false;
      }

      ORawPair<Object, ORID> result;

      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && comparator.compare(nextKey, lastKey) > 0) {
          nextKey = null;
        }

      } while (result == null && nextKey != null);

      if (result == null) {
        return false;
      }

      final byte[] normalizedKey = normalizeKey(result.first);
      action.accept(new ORawPair<>(normalizedKey, result.second));

      return true;
    }

    @Override
    public Spliterator<ORawPair<byte[], ORID>> trySplit() {
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
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (entryOne, entryTwo) ->
          ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  private class PureTxBetweenIndexBackwardSpliterator
      implements Spliterator<ORawPair<byte[], ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private Object firstKey;

    private Object nextKey;

    private PureTxBetweenIndexBackwardSpliterator(
        Object fromKey,
        boolean fromInclusive,
        Object toKey,
        boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      if (fromKey != null) {
        fromKey = enhanceFromCompositeKeyBetweenDesc(fromKey, fromInclusive);
      }
      if (toKey != null) {
        toKey = enhanceToCompositeKeyBetweenDesc(toKey, toInclusive);
      }

      final Object[] keys =
          indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
      if (keys.length == 0) {
        nextKey = null;
      } else {
        firstKey = keys[0];
        nextKey = keys[1];
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<byte[], ORID>> action) {
      if (nextKey == null) {
        return false;
      }

      ORawPair<Object, ORID> result;
      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && comparator.compare(nextKey, firstKey) < 0) nextKey = null;
      } while (result == null && nextKey != null);

      if (result == null) {
        return false;
      }

      final byte[] normalizedKey = normalizeKey(result.first);

      action.accept(new ORawPair<>(normalizedKey, result.second));
      return true;
    }

    @Override
    public Spliterator<ORawPair<byte[], ORID>> trySplit() {
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
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (entryOne, entryTwo) ->
          -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, final OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    ORID result = backendValue;
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      if (backendValue == null) {
        return null;
      } else {
        return new ORawPair<>(key, backendValue);
      }
    }

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
        changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
        result = null;
      } else if (entry.getOperation() == OTransactionIndexChanges.OPERATION.PUT)
        result = entry.getValue().getIdentity();
    }

    if (result == null) {
      return null;
    }

    return new ORawPair<>(key, result);
  }

  @Deprecated
  @Override
  public OIdentifiable get(Object key) {
    final Iterator<ORID> iterator;
    try (Stream<ORID> stream = getRids(key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }

    return null;
  }

  @Override
  public Stream<ORID> getRids(final Object key) {
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.getRids(key);
    }

    final Object collatedKey = getCollatingValue(key);

    ORID rid;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      //noinspection resource
      rid = super.getRids(collatedKey).findFirst().orElse(null);
    } else {
      rid = null;
    }

    final ORawPair<Object, ORID> txIndexEntry = calculateTxIndexEntry(key, rid, indexChanges);
    if (txIndexEntry == null) {
      return Stream.empty();
    }

    return IndexStreamSecurityDecorator.decorateRidStream(this, Stream.of(txIndexEntry.second));
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> stream() {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.stream();
    }

    final Stream<ORawPair<byte[], ORID>> txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexForwardSpliterator(null, true, null, true, indexChanges), false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream = indexInternalOriginalKeyDelegate.stream();
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, true));
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> descStream() {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.descStream();
    }

    final Stream<ORawPair<byte[], ORID>> txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexBackwardSpliterator(null, true, null, true, indexChanges), false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream =
        indexInternalOriginalKeyDelegate.descStream();
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, false));
  }

  @Override
  public Stream<byte[]> keyStream() {
    return stream().map(pair -> pair.first);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesBetween(
      Object fromKey,
      final boolean fromInclusive,
      Object toKey,
      final boolean toInclusive,
      final boolean ascOrder) {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.streamEntriesBetween(
          fromKey, fromInclusive, toKey, toInclusive, ascOrder);
    }

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<byte[], ORID>> txStream;
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream =
        indexInternalOriginalKeyDelegate.streamEntriesBetween(
            fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
    }

    fromKey = getCollatingValue(fromKey);

    final Stream<ORawPair<byte[], ORID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream =
        indexInternalOriginalKeyDelegate.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.streamEntriesMinor(toKey, toInclusive, ascOrder);
    }

    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<byte[], ORID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream =
        indexInternalOriginalKeyDelegate.streamEntriesMinor(toKey, toInclusive, ascOrder);
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Collator getCollator() {
    return collator;
  }

  @Override
  public KeyNormalizers getKeyNormalizers() {
    return keyNormalizers;
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.streamEntries(keys, ascSortOrder);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> txStream =
        keys.stream()
            .map((key) -> calculateTxIndexEntry(getCollatingValue(key), null, indexChanges))
            .filter(Objects::nonNull)
            .map(pair -> new ORawPair<>(normalizeKey(pair.first), pair.second))
            .sorted(
                (entryOne, entryTwo) -> {
                  if (ascSortOrder) {
                    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
                  } else {
                    return -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
                  }
                });

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedStream =
        indexInternalOriginalKeyDelegate.streamEntries(keys, ascSortOrder);
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascSortOrder));
  }

  private Stream<ORawPair<byte[], ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<byte[], ORID>> txStream,
      Stream<ORawPair<byte[], ORID>> backedStream,
      boolean ascSortOrder) {
    return com.orientechnologies.common.spliterators.Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map(
                (entry) ->
                    calculateTxIndexEntry(
                        getCollatingValue(entry.first), entry.second, indexChanges))
            .filter(Objects::nonNull)
            .map(pair -> new ORawPair<>(normalizeKey(pair.first), pair.second)),
        (entryOne, entryTwo) -> {
          if (ascSortOrder) {
            return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
          } else {
            return -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
          }
        });
  }

  private byte[] normalizeKey(final Object key) {
    if (key instanceof OCompositeKey) {
      final OCompositeKey compositeKey = (OCompositeKey) key;

      assert keyTypes.length == compositeKey.getKeys().size();

      return keyNormalizers.normalize((OCompositeKey) key, keyTypes);
    }

    assert keyTypes.length == 1;
    return keyNormalizers.normalize(key, keyTypes[0]);
  }
}
