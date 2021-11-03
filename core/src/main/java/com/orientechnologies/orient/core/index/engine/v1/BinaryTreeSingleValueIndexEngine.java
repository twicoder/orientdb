package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.engine.BaseBinaryKeyIndexEngine;
import com.orientechnologies.orient.core.index.engine.SingleValueBinaryKeyIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree.BinaryBTree;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

public class BinaryTreeSingleValueIndexEngine
    implements SingleValueBinaryKeyIndexEngine, BinaryTreeIndexEngine {
  private static final String DATA_FILE_EXTENSION = ".bbt";

  private final String name;
  private final int id;

  private final BinaryBTree bTree;
  private final KeyNormalizers keyNormalizers;

  private volatile OType[] keyTypes;

  public BinaryTreeSingleValueIndexEngine(
      final String name,
      final int id,
      final OAbstractPaginatedStorage storage,
      final int spliteratorCacheSize,
      final int maxKeySize,
      final int maxSearchDepth,
      final Locale locale,
      final int decomposition) {
    this.name = name;
    this.id = id;

    this.bTree =
        new BinaryBTree(
            spliteratorCacheSize, maxKeySize, maxSearchDepth, storage, name, DATA_FILE_EXTENSION);
    this.keyNormalizers = new KeyNormalizers(locale, decomposition);
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {}

  @Override
  public void flush() {}

  @Override
  public void create(
      OAtomicOperation atomicOperation,
      @SuppressWarnings("rawtypes") OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      @SuppressWarnings("rawtypes") OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption)
      throws IOException {
    if (keySize != keyTypes.length) {
      throw new IllegalStateException(
          "Amount of key types does not match to key size " + keyTypes.length + " vs " + keySize);
    }
    bTree.create(atomicOperation);
    this.keyTypes = keyTypes;
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) throws IOException {
    bTree.delete(atomicOperation);
  }

  private void doClearTree(OAtomicOperation atomicOperation) {
    try (Stream<ORawPair<byte[], ORID>> stream = bTree.allEntries()) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) throws IOException {
    doClearTree(atomicOperation);
  }

  @Override
  public void close() {}

  @Override
  public long size(ValuesTransformer transformer) {
    return bTree.size();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    bTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, ORID value) {
    final byte[] normalizedKey = normalizeKey(key);
    bTree.put(atomicOperation, normalizedKey, value);
  }

  @Override
  public Stream<ORID> get(Object key) {
    final byte[] normalizedKey = normalizeKey(key);
    final ORID rid = bTree.get(normalizedKey);
    if (rid == null) {
      return Stream.empty();
    }

    return Stream.of(rid);
  }

  @Override
  public void load(
      String name,
      int keySize,
      OType[] keyTypes,
      @SuppressWarnings("rawtypes") OBinarySerializer keySerializer,
      OEncryption encryption) {
    if (keySize != keyTypes.length) {
      throw new IllegalStateException(
          "Amount of key types does not match to key size " + keyTypes.length + " vs " + keySize);
    }

    bTree.load(name);
    this.keyTypes = keyTypes;
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator) {
    final byte[] normalizedKey = normalizeKey(key);
    return bTree.validatedPut(atomicOperation, key, normalizedKey, value, validator);
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) throws IOException {
    final byte[] normalizedKey = normalizeKey(key);
    return bTree.remove(atomicOperation, normalizedKey) != null;
  }

  private byte[] normalizeKey(Object key) {
    final byte[] normalizedKey;

    if (key instanceof OCompositeKey) {
      final OCompositeKey compositeKey = (OCompositeKey) key;
      assert compositeKey.getKeys().size() == keyTypes.length;

      normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);
    } else {
      assert keyTypes.length == 1;

      normalizedKey = keyNormalizers.normalize(key, keyTypes[0]);
    }
    return normalizedKey;
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {

    final Object enhancedKeyFrom;
    final Object enhancedKeyTo;

    if (ascSortOrder) {
      enhancedKeyFrom =
          BaseBinaryKeyIndexEngine.enhanceFromCompositeKeyBetweenAsc(
              rangeFrom, fromInclusive, keyTypes.length);
      enhancedKeyTo =
          BaseBinaryKeyIndexEngine.enhanceToCompositeKeyBetweenAsc(
              rangeTo, toInclusive, keyTypes.length);
    } else {
      enhancedKeyFrom =
          BaseBinaryKeyIndexEngine.enhanceToCompositeKeyBetweenDesc(
              rangeFrom, fromInclusive, keyTypes.length);
      enhancedKeyTo =
          BaseBinaryKeyIndexEngine.enhanceFromCompositeKeyBetweenDesc(
              rangeTo, toInclusive, keyTypes.length);
    }

    final byte[] normalizedKeyFrom = normalizeKey(enhancedKeyFrom);
    final byte[] normalizedKeyTo = normalizeKey(enhancedKeyTo);

    return bTree.iterateEntriesBetween(
        normalizedKeyFrom, fromInclusive, normalizedKeyTo, toInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    final Object enhancedKey;

    if (ascSortOrder) {
      enhancedKey =
          BaseBinaryKeyIndexEngine.enhanceCompositeKeyMajorAsc(
              fromKey, isInclusive, keyTypes.length);
    } else {
      enhancedKey =
          BaseBinaryKeyIndexEngine.enhanceCompositeKeyMajorDesc(
              fromKey, isInclusive, keyTypes.length);
    }

    final byte[] normalizedKey = normalizeKey(enhancedKey);

    return bTree.iterateEntriesMajor(normalizedKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    final Object enhancedKey;
    if (ascSortOrder) {
      enhancedKey =
          BaseBinaryKeyIndexEngine.enhanceCompositeKeyMinorAsc(toKey, isInclusive, keyTypes.length);
    } else {
      enhancedKey =
          BaseBinaryKeyIndexEngine.enhanceCompositeKeyMinorDesc(
              toKey, isInclusive, keyTypes.length);
    }

    final byte[] normalizedKey = normalizeKey(enhancedKey);

    return bTree.iterateEntriesMinor(normalizedKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> stream(ValuesTransformer valuesTransformer) {
    final byte[] firstKey = bTree.firstKey();
    if (firstKey == null) {
      return Stream.empty();
    }

    return bTree.iterateEntriesMajor(firstKey, true, true);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> descStream(ValuesTransformer valuesTransformer) {
    final byte[] lastKey = bTree.lastKey();
    if (lastKey == null) {
      return Stream.empty();
    }

    return bTree.iterateEntriesMinor(lastKey, true, false);
  }

  @Override
  public Stream<byte[]> keyStream() {
    return stream(null).map(pair -> pair.first);
  }
}
