package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.engine.BaseBinaryKeyIndexEngine;
import com.orientechnologies.orient.core.index.engine.MultiValueBinaryKeyIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree.BinaryBTree;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

public class BinaryTreeMultiValueIndexEngine implements MultiValueBinaryKeyIndexEngine {
  public static final String DATA_FILE_EXTENSION = ".bbt";

  private final String name;
  private final int id;

  private final BinaryBTree bTree;

  private volatile OType[] keyTypes;

  private final KeyNormalizers keyNormalizers;

  public BinaryTreeMultiValueIndexEngine(
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

    this.keyTypes = extendKeyTypes(keyTypes);
  }

  private OType[] extendKeyTypes(OType[] keyTypes) {
    final OType[] types = Arrays.copyOf(keyTypes, keyTypes.length + 1);
    types[types.length - 1] = OType.LINK;
    return types;
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) throws IOException {
    doClearTree(atomicOperation);
    bTree.delete(atomicOperation);
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) throws IOException {
    doClearTree(atomicOperation);
  }

  private void doClearTree(OAtomicOperation atomicOperation) {
    try (Stream<ORawPair<byte[], ORID>> stream = bTree.allEntries()) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }
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
  public boolean remove(OAtomicOperation atomicOperation, Object key, ORID value) {
    final OCompositeKey compositeKey = createCompositeKey(key, value);
    final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

    return bTree.remove(atomicOperation, normalizedKey) != null;
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, ORID value) {
    final OCompositeKey compositeKey = createCompositeKey(key, value);
    final byte[] normalizedKey = keyNormalizers.normalize(compositeKey, keyTypes);

    bTree.put(atomicOperation, normalizedKey, value);
  }

  @Override
  public Stream<ORID> get(Object key) {
    final OCompositeKey compositeKey = convertToCompositeKey(key);

    final OCompositeKey enhancedKeyFrom =
        (OCompositeKey)
            BaseBinaryKeyIndexEngine.enhanceFromCompositeKeyBetweenAsc(
                compositeKey, true, keyTypes.length);
    final OCompositeKey enhancedKeyTo =
        (OCompositeKey)
            BaseBinaryKeyIndexEngine.enhanceToCompositeKeyBetweenAsc(
                compositeKey, true, keyTypes.length);

    final byte[] normalizedKeyFrom = keyNormalizers.normalize(enhancedKeyFrom, keyTypes);
    final byte[] normalizedKeyTo = keyNormalizers.normalize(enhancedKeyTo, keyTypes);

    return bTree
        .iterateEntriesBetween(normalizedKeyFrom, true, normalizedKeyTo, true, true)
        .map(pair -> pair.second);
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
    this.keyTypes = extendKeyTypes(keyTypes);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    final OCompositeKey compositeKeyFrom = convertToCompositeKey(rangeFrom);
    final OCompositeKey compositeKeyTo = convertToCompositeKey(rangeTo);

    final OCompositeKey enhancedKeyFrom;
    final OCompositeKey enhancedKeyTo;

    if (ascSortOrder) {
      enhancedKeyFrom =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceFromCompositeKeyBetweenAsc(
                  compositeKeyFrom, fromInclusive, keyTypes.length);
      enhancedKeyTo =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceToCompositeKeyBetweenAsc(
                  compositeKeyTo, toInclusive, keyTypes.length);
    } else {
      enhancedKeyFrom =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceFromCompositeKeyBetweenDesc(
                  compositeKeyFrom, fromInclusive, keyTypes.length);
      enhancedKeyTo =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceToCompositeKeyBetweenDesc(
                  compositeKeyTo, toInclusive, keyTypes.length);
    }

    final byte[] normalizedKeyFrom = keyNormalizers.normalize(enhancedKeyFrom, keyTypes);
    final byte[] normalizedKeyTo = keyNormalizers.normalize(enhancedKeyTo, keyTypes);

    return bTree.iterateEntriesBetween(
        normalizedKeyFrom, fromInclusive, normalizedKeyTo, toInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    final OCompositeKey compositeKey = convertToCompositeKey(fromKey);

    final OCompositeKey enhancedKey;
    if (ascSortOrder) {
      enhancedKey =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceCompositeKeyMajorAsc(
                  compositeKey, isInclusive, keyTypes.length);
    } else {
      enhancedKey =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceCompositeKeyMajorDesc(
                  compositeKey, isInclusive, keyTypes.length);
    }

    final byte[] normalizedKey = keyNormalizers.normalize(enhancedKey, keyTypes);

    return bTree.iterateEntriesMajor(normalizedKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    final OCompositeKey compositeKey = convertToCompositeKey(toKey);

    final OCompositeKey enhancedKey;
    if (ascSortOrder) {
      enhancedKey =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceCompositeKeyMinorAsc(
                  compositeKey, isInclusive, keyTypes.length);
    } else {
      enhancedKey =
          (OCompositeKey)
              BaseBinaryKeyIndexEngine.enhanceCompositeKeyMinorDesc(
                  compositeKey, isInclusive, keyTypes.length);
    }

    final byte[] normalizeKey = keyNormalizers.normalize(enhancedKey, keyTypes);

    return bTree.iterateEntriesMinor(normalizeKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<byte[], ORID>> stream(ValuesTransformer valuesTransformer) {
    return bTree.allEntries();
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

  private static OCompositeKey createCompositeKey(final Object key, final ORID value) {
    final OCompositeKey compositeKey = new OCompositeKey(key);
    compositeKey.addKey(value);
    return compositeKey;
  }

  private static OCompositeKey convertToCompositeKey(Object rangeFrom) {
    OCompositeKey firstKey;
    if (rangeFrom instanceof OCompositeKey) {
      firstKey = (OCompositeKey) rangeFrom;
    } else {
      firstKey = new OCompositeKey(rangeFrom);
    }
    return firstKey;
  }

  @Override
  public boolean rawRemove(OAtomicOperation atomicOperation, byte[] key, ORID value)
      throws IOException {
    return bTree.remove(atomicOperation, key) != null;
  }
}
