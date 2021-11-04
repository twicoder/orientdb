package com.orientechnologies.orient.core.index;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OMixedIndexRIDContainerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexNotUniqueBinaryKey extends OIndexAbstract implements IndexInternalBinaryKey {
  private final Collator collator;
  private final KeyNormalizers keyNormalizers;

  public IndexNotUniqueBinaryKey(
      String name,
      String type,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version,
      OStorage storage,
      int binaryFormatVersion) {
    super(
        name,
        type,
        algorithm,
        valueContainerAlgorithm,
        metadata,
        version,
        storage,
        binaryFormatVersion);

    final ORawPair<Collator, KeyNormalizers> pair =
        IndexInternalBinaryKey.createCollatorNormalizers(storage, metadata);

    collator = pair.first;
    keyNormalizers = pair.second;
  }

  @Deprecated
  @Override
  public Collection<ORID> get(Object key) {
    final List<ORID> rids;
    try (Stream<ORID> stream = getRids(key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      Stream<ORID> stream;
      while (true) {
        try {
          //noinspection resource
          stream = storage.getIndexValues(indexId, key);
          return IndexStreamSecurityDecorator.decorateRidStream(this, stream);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  public IndexNotUniqueBinaryKey put(Object key, final OIdentifiable singleValue) {
    key = getCollatingValue(key);

    acquireSharedLock();

    try {
      final ORID identity = singleValue.getIdentity();

      while (true) {
        try {
          doPut(storage, key, identity);
          return this;
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    storage.putRidIndexEntry(indexId, key, rid);
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return doRemove(storage, key, value.getIdentity());
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    return storage.removeRidIndexEntry(indexId, key, rid);
  }

  public IndexNotUniqueBinaryKey create(
      final String name,
      final OIndexDefinition indexDefinition,
      final String clusterIndexName,
      final Set<String> clustersToIndex,
      boolean rebuild,
      final OProgressListener progressListener) {

    return (IndexNotUniqueBinaryKey)
        super.create(
            indexDefinition,
            clusterIndexName,
            clustersToIndex,
            rebuild,
            progressListener,
            determineValueSerializer());
  }

  @SuppressWarnings("rawtypes")
  protected OBinarySerializer determineValueSerializer() {
    if (binaryFormatVersion >= 13) {
      return storage
          .getComponentsFactory()
          .binarySerializerFactory
          .getObjectSerializer(OMixedIndexRIDContainerSerializer.ID);
    }

    return storage
        .getComponentsFactory()
        .binarySerializerFactory
        .getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this,
              storage.iterateBinaryIndexEntriesBetween(
                  indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this,
              storage.iterateBinaryIndexEntriesMajor(
                  indexId, fromKey, fromInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this,
              storage.iterateBinaryIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
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
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder) {
      comparator = ODefaultComparator.INSTANCE;
    } else {
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);
    }

    sortedKeys.sort(comparator);

    //noinspection resource
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this,
        sortedKeys.stream()
            .flatMap(
                (key) -> {
                  key = getCollatingValue(key);

                  acquireSharedLock();
                  try {
                    while (true) {
                      try {
                        return storage.getIndexBinaryEntries(indexId, key);
                      } catch (OInvalidIndexEngineIdException ignore) {
                        doReloadIndexEngine();
                      }
                    }

                  } finally {
                    releaseSharedLock();
                  }
                }));
  }

  public long size() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> stream() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this, storage.getIndexBinaryStream(indexId, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<byte[]> keyStream() {
    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.getIndexBinaryKeyStream(indexId);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> descStream() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this, storage.getIndexBinaryDescStream(indexId, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    while (true) {
      try {
        return storage.hasIndexRangeQuerySupport(indexId);
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.NonUnique);
  }
}
