package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.BaseIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.*;
import java.util.stream.Stream;

public class IndexUniqueBinaryKey extends OIndexAbstract implements IndexInternalBinaryKey {
  private final BaseIndexEngine.Validator<Object, ORID> uniqueValidator =
      (key, oldValue, newValue) -> {
        if (oldValue != null) {
          // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
          if (!oldValue.equals(newValue)) {
            final Boolean mergeSameKey =
                metadata != null ? (Boolean) metadata.field(OIndex.MERGE_KEYS) : Boolean.FALSE;
            if (mergeSameKey == null || !mergeSameKey) {
              throw new ORecordDuplicatedException(
                  String.format(
                      "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                      newValue.getIdentity(), key, getName(), oldValue.getIdentity()),
                  getName(),
                  oldValue.getIdentity(),
                  key);
            }
          } else {
            return BaseIndexEngine.Validator.IGNORE;
          }
        }

        if (!newValue.getIdentity().isPersistent()) {
          newValue = newValue.getRecord();
        }
        return newValue.getIdentity();
      };

  public IndexUniqueBinaryKey(
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
  }

  @Deprecated
  @Override
  public Object get(Object key) {
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
  public IndexUniqueBinaryKey put(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      while (true) {
        try {
          doPut(storage, key, value.getIdentity());
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
      return this;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      while (true)
        try {
          final Stream<ORID> stream = storage.getIndexValues(indexId, key);
          return IndexStreamSecurityDecorator.decorateRidStream(this, stream);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  public IndexUniqueBinaryKey create(
      final String name,
      final OIndexDefinition indexDefinition,
      final String clusterIndexName,
      final Set<String> clustersToIndex,
      boolean rebuild,
      final OProgressListener progressListener) {
    return (IndexUniqueBinaryKey)
        super.create(
            indexDefinition,
            clusterIndexName,
            clustersToIndex,
            rebuild,
            progressListener,
            determineValueSerializer());
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder) comparator = ODefaultComparator.INSTANCE;
    else comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    sortedKeys.sort(comparator);

    //noinspection resource
    return IndexStreamSecurityDecorator.decorateBinaryStream(
        this,
        sortedKeys.stream()
            .flatMap(
                (key) -> {
                  final Object collatedKey = getCollatingValue(key);
                  acquireSharedLock();
                  try {
                    while (true) {
                      try {
                        return storage.getIndexBinaryEntries(indexId, collatedKey);
                      } catch (OInvalidIndexEngineIdException ignore) {
                        doReloadIndexEngine();
                      }
                    }
                  } finally {
                    releaseSharedLock();
                  }
                }));
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true)
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this,
              storage.iterateBinaryIndexEntriesBetween(
                  indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
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
      while (true)
        try {
          return IndexStreamSecurityDecorator.decorateBinaryStream(
              this,
              storage.iterateBinaryIndexEntriesMajor(
                  indexId, fromKey, fromInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
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
  public boolean isUnique() {
    return true;
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    storage.validatedPutIndexValue(indexId, key, rid, uniqueValidator);
  }

  @Override
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
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Unique);
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected OBinarySerializer determineValueSerializer() {
    return OStreamSerializerRID.INSTANCE;
  }
}
