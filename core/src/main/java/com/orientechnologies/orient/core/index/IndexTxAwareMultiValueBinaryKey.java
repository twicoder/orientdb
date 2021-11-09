package com.orientechnologies.orient.core.index;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.CollatorComparator;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexTxAwareMultiValueBinaryKey extends OIndexTxAware<Collection<OIdentifiable>>
    implements IndexInternalBinaryKey {
  private final Collator collator;
  private final KeyNormalizers keyNormalizers;
  private final OType[] keyTypes;
  private final CollatorComparator comparator;

  public IndexTxAwareMultiValueBinaryKey(
      final ODatabaseDocumentInternal database,
      final IndexInternal delegate,
      final Collator collator,
      final KeyNormalizers keyNormalizers,
      final OType[] keyTypes) {
    super(database, delegate);

    this.collator = collator;
    this.keyNormalizers = keyNormalizers;
    this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length + 1);
    this.keyTypes[keyTypes.length] = OType.LINK;

    this.comparator = new CollatorComparator(collator);
  }

  private class PureTxBetweenIndexForwardSpliterator
      implements Spliterator<ORawPair<byte[], ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private Object lastKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<>();
    private Object key;

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
      if (valuesIterator.hasNext()) {
        final ORawPair<byte[], ORID> entry = nextEntryInternal();
        action.accept(entry);
        return true;
      }

      if (nextKey == null) {
        return false;
      }

      TreeSet<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && comparator.compare(nextKey, lastKey) > 0) nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty()) {
        return false;
      }

      valuesIterator = result.iterator();
      final ORawPair<byte[], ORID> entry = nextEntryInternal();
      action.accept(entry);

      return true;
    }

    private ORawPair<byte[], ORID> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      final ORID rid = identifiable.getIdentity();

      final OCompositeKey compositeKey = new OCompositeKey(key, rid);
      final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

      return new ORawPair<>(normalizedKey, rid);
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
      return NONNULL | ORDERED | SORTED;
    }

    @Override
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (entryOne, entryTwo) ->
          ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  private class PureTxBetweenIndexBackwardCursor implements Spliterator<ORawPair<byte[], ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private Object firstKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<>();
    private Object key;

    private PureTxBetweenIndexBackwardCursor(
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

    private ORawPair<byte[], ORID> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      final ORID rid = identifiable.getIdentity();

      final OCompositeKey compositeKey = new OCompositeKey(key, rid);
      final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

      return new ORawPair<>(normalizedKey, identifiable.getIdentity());
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<byte[], ORID>> action) {
      if (valuesIterator.hasNext()) {
        final ORawPair<byte[], ORID> entry = nextEntryInternal();
        action.accept(entry);
        return true;
      }

      if (nextKey == null) {
        return false;
      }

      TreeSet<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && comparator.compare(nextKey, firstKey) < 0) nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty()) {
        return false;
      }

      valuesIterator = result.iterator();
      final ORawPair<byte[], ORID> entry = nextEntryInternal();
      action.accept(entry);
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
      return NONNULL | ORDERED | SORTED;
    }

    @Override
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (entryOne, entryTwo) ->
          -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  @Deprecated
  @Override
  public Collection<OIdentifiable> get(Object key) {
    final List<OIdentifiable> rids;
    try (Stream<ORID> stream = getRids(key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  @Override
  public Stream<ORID> getRids(final Object key) {
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.getRids(key);
    }

    final Object collatedKey = getCollatingValue(key);
    Set<OIdentifiable> txChanges = calculateTxValue(collatedKey, indexChanges);

    @SuppressWarnings("resource")
    final Stream<ORID> backedStream = super.getRids(collatedKey);
    if (txChanges == null) {
      txChanges = Collections.emptySet();
    }

    //noinspection resource
    return IndexStreamSecurityDecorator.decorateRidStream(
        this,
        Stream.concat(
            backedStream
                .map((rid) -> calculateTxIndexEntry(collatedKey, rid, indexChanges))
                .filter(Objects::nonNull)
                .map((pair) -> pair.second),
            txChanges.stream().map(OIdentifiable::getIdentity)));
  }

  @Override
  public OIndex put(Object key, OIdentifiable value) {
    final OCompositeKey compositeKey = new OCompositeKey(key, value);
    final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

    return doPut(key, normalizedKey, value);
  }

  @Override
  public boolean remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object key, OIdentifiable rid) {
    final OCompositeKey compositeKey = new OCompositeKey(key, rid);
    final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

    return doRemove(key, normalizedKey, rid);
  }

  @Override
  protected OIndexTxAware<Collection<OIdentifiable>> doPut(
      Object key, byte[] normalizedKey, OIdentifiable value) {
    return super.doPut(key, normalizedKey, value);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> stream() {
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());

    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;

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
    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;

    if (indexChanges == null) {
      return indexInternalOriginalKeyDelegate.descStream();
    }

    final Stream<ORawPair<byte[], ORID>> txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexBackwardCursor(null, true, null, true, indexChanges), false);

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

    final OTransactionIndexChanges indexChanges =
        database.getMicroOrRegularTransaction().getIndexChangesInternal(delegate.getName());
    final IndexInternalBinaryKey indexInternalOriginalKeyDelegate =
        (IndexInternalBinaryKey) delegate;

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
              new PureTxBetweenIndexBackwardCursor(
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
              new PureTxBetweenIndexBackwardCursor(
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
              new PureTxBetweenIndexBackwardCursor(
                  firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedCursor =
        indexInternalOriginalKeyDelegate.streamEntriesMinor(toKey, toInclusive, ascOrder);
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedCursor, ascOrder));
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
            .flatMap(
                (key) -> {
                  final TreeSet<OIdentifiable> result =
                      calculateTxValue(getCollatingValue(key), indexChanges);
                  if (result != null) {
                    return result.stream()
                        .map((rid) -> new ORawPair<>(getCollatingValue(key), rid.getIdentity()));
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .map(
                pair -> {
                  final OCompositeKey compositeKey = new OCompositeKey(pair.first, pair.second);
                  final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);
                  return new ORawPair<>(normalizedKey, pair.second);
                })
            .sorted(
                (entryOne, entryTwo) -> {
                  if (ascSortOrder) {
                    return ODefaultComparator.INSTANCE.compare(
                        getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
                  } else {
                    return -ODefaultComparator.INSTANCE.compare(
                        getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
                  }
                });

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateBinaryStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<byte[], ORID>> backedCursor =
        indexInternalOriginalKeyDelegate.streamEntries(keys, ascSortOrder);
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, backedCursor, ascSortOrder));
  }

  private Stream<ORawPair<byte[], ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<byte[], ORID>> txStream,
      Stream<ORawPair<byte[], ORID>> backedStream,
      boolean ascOrder) {
    return com.orientechnologies.common.spliterators.Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map((entry) -> calculateTxIndexBinaryEntry(entry.first, entry.second, indexChanges))
            .filter(Objects::nonNull),
        (entryOne, entryTwo) -> {
          if (ascOrder) {
            return ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          } else {
            return -ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          }
        });
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);

    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
        changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OTransactionIndexChanges.OPERATION.PUT
          && entry.getValue().equals(backendValue)) {
        putCounter++;
      } else if (entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
        if (entry.getValue() == null) {
          putCounter = 0;
        } else if (entry.getValue().equals(backendValue) && putCounter > 0) {
          putCounter--;
        }
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  private ORawPair<byte[], ORID> calculateTxIndexBinaryEntry(
      byte[] key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerBinaryKey(key);

    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
        changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OTransactionIndexChanges.OPERATION.PUT
          && entry.getValue().equals(backendValue)) {
        putCounter++;
      } else if (entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
        if (entry.getValue() == null) {
          putCounter = 0;
        } else if (entry.getValue().equals(backendValue) && putCounter > 0) {
          putCounter--;
        }
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  private static TreeSet<OIdentifiable> calculateTxValue(
      final Object key, OTransactionIndexChanges indexChanges) {
    final List<OIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return null;
    }

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
        changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
        if (entry.getValue() == null) {
          result.clear();
        } else {
          result.remove(entry.getValue());
        }
      } else {
        result.add(entry.getValue());
      }
    }

    if (result.isEmpty()) {
      return null;
    }

    return new TreeSet<>(result);
  }
}
