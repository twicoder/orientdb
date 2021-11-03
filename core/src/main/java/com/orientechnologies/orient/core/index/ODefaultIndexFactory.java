/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.engine.BaseIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.engine.ORemoteIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import java.util.*;

/**
 * Default OrientDB index factory for indexes based on SBTree.<br>
 * Supports index types:
 *
 * <ul>
 *   <li>UNIQUE
 *   <li>NOTUNIQUE
 *   <li>FULLTEXT
 *   <li>DICTIONARY
 * </ul>
 */
public class ODefaultIndexFactory implements OIndexFactory {

  private static final String SBTREE_ALGORITHM = "SBTREE";
  static final String SBTREE_BONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String NONE_VALUE_CONTAINER = "NONE";
  static final String CELL_BTREE_ALGORITHM = "CELL_BTREE";

  public static final String BINARY_TREE_ALGORITHM = "BINARY_TREE";

  public static final String BINARY_TREE_LOCALE = "locale";
  public static final String BINARY_TREE_DECOMPOSITION = "decomposition";

  private static final int SPLITERATOR_CACHE_SIZE =
      OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

  private static final int MAX_KEY_SIZE =
      OGlobalConfiguration.BINARY_TREE_MAX_KEY_SIZE.getValueAsInteger();

  private static final int MAX_SEARCH_DEPTH = 64;

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(SBTREE_ALGORITHM);
    algorithms.add(CELL_BTREE_ALGORITHM);
    algorithms.add(BINARY_TREE_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  static boolean isMultiValueIndex(final String indexType) {
    switch (OClass.INDEX_TYPE.valueOf(indexType)) {
      case UNIQUE:
      case DICTIONARY:
        return false;
    }

    return true;
  }

  /**
   * Index types :
   *
   * <ul>
   *   <li>UNIQUE
   *   <li>NOTUNIQUE
   *   <li>FULLTEXT
   *   <li>DICTIONARY
   * </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public IndexInternal createIndex(
      String name,
      OStorage storage,
      String indexType,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version)
      throws OConfigurationException {
    if (valueContainerAlgorithm == null) {
      valueContainerAlgorithm = NONE_VALUE_CONTAINER;
    }

    OClass.INDEX_TYPE passedIndexType = OClass.INDEX_TYPE.valueOf(indexType);

    if (algorithm.equals(BINARY_TREE_ALGORITHM)
        && (passedIndexType == OClass.INDEX_TYPE.DICTIONARY
            || passedIndexType == OClass.INDEX_TYPE.FULLTEXT)) {
      algorithm = CELL_BTREE_ALGORITHM;
    }

    if (version < 0) {
      version = getLastVersion(algorithm);
    }

    if (algorithm.equals(CELL_BTREE_ALGORITHM) || algorithm.equals(SBTREE_ALGORITHM)) {
      return createSBTreeIndex(
          name,
          indexType,
          valueContainerAlgorithm,
          metadata,
          (OAbstractPaginatedStorage) storage.getUnderlying(),
          version,
          algorithm);
    }

    return createBinaryTreeIndex(
        name, indexType, metadata, (OAbstractPaginatedStorage) storage, version, algorithm);
  }

  private IndexInternal createBinaryTreeIndex(
      String name,
      String indexType,
      ODocument metadata,
      OAbstractPaginatedStorage storage,
      int version,
      String algorithm) {
    final int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new IndexUniqueBinaryKey(
          name,
          indexType,
          algorithm,
          SBTREE_BONSAI_VALUE_CONTAINER,
          metadata,
          version,
          storage,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new IndexNotUniqueBinaryKey(
          name,
          indexType,
          algorithm,
          SBTREE_BONSAI_VALUE_CONTAINER,
          metadata,
          version,
          storage,
          binaryFormatVersion);
    }

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  private static IndexInternal createSBTreeIndex(
      String name,
      String indexType,
      String valueContainerAlgorithm,
      ODocument metadata,
      OAbstractPaginatedStorage storage,
      int version,
      String algorithm) {

    final int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new IndexUniqueOriginalKey(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new IndexNotUniqueOriginalKey(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      OLogManager.instance()
          .warnNoDb(
              ODefaultIndexFactory.class,
              "You are creating native full text index instance. "
                  + "That is unsafe because this type of index is deprecated and will be removed in future.");
      return new OIndexFullText(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    }

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    switch (algorithm) {
      case SBTREE_ALGORITHM:
        return OSBTreeIndexEngine.VERSION;
      case CELL_BTREE_ALGORITHM:
        return CellBTreeIndexEngine.VERSION;
      case BINARY_TREE_ALGORITHM:
        return BinaryTreeIndexEngine.VERSION;
    }

    throw new IllegalStateException("Invalid algorithm name " + algorithm);
  }

  @Override
  public BaseIndexEngine createIndexEngine(
      int indexId,
      String algorithm,
      String name,
      Boolean durableInNonTxMode,
      OStorage storage,
      int version,
      int apiVersion,
      boolean multiValue,
      Map<String, String> engineProperties) {

    if (algorithm == null) {
      throw new OIndexException("Name of algorithm is not specified");
    }
    final BaseIndexEngine indexEngine;
    String storageType = storage.getType();

    if (storageType.equals("distributed")) {
      storage = storage.getUnderlying();
      storageType = storage.getType();
    }

    switch (storageType) {
      case "memory":
      case "plocal":
        switch (algorithm) {
          case SBTREE_ALGORITHM:
            indexEngine =
                new OSBTreeIndexEngine(indexId, name, (OAbstractPaginatedStorage) storage, version);
            break;
          case CELL_BTREE_ALGORITHM:
            if (multiValue) {
              indexEngine =
                  new CellBTreeMultiValueOriginalKeyIndexEngine(
                      indexId, name, (OAbstractPaginatedStorage) storage, version);
            } else {
              indexEngine =
                  new CellBTreeSingleValueOriginalKeyIndexEngine(
                      indexId, name, (OAbstractPaginatedStorage) storage, version);
            }
            break;
          default:
            throw new IllegalStateException("Invalid name of algorithm :'" + "'");
        }
        break;
      case BINARY_TREE_ALGORITHM:
        final Locale locale;
        final String languageTag = engineProperties.get(BINARY_TREE_LOCALE);
        if (languageTag == null) {
          locale = storage.getConfiguration().getLocaleInstance();
        } else {
          locale = Locale.forLanguageTag(languageTag);
        }

        int decomposition = Collator.NO_DECOMPOSITION;
        final String decompositionTag = engineProperties.get(BINARY_TREE_DECOMPOSITION);

        if (decompositionTag != null) {
          try {
            decomposition = Integer.parseInt(decompositionTag);
          } catch (NumberFormatException e) {
            // ignore
          }
        }

        if (multiValue) {
          indexEngine =
              new BinaryTreeMultiValueIndexEngine(
                  name,
                  indexId,
                  (OAbstractPaginatedStorage) storage,
                  SPLITERATOR_CACHE_SIZE,
                  MAX_KEY_SIZE,
                  MAX_SEARCH_DEPTH,
                  locale,
                  decomposition);
        } else {
          indexEngine =
              new BinaryTreeSingleValueIndexEngine(
                  name,
                  indexId,
                  (OAbstractPaginatedStorage) storage,
                  SPLITERATOR_CACHE_SIZE,
                  MAX_KEY_SIZE,
                  MAX_SEARCH_DEPTH,
                  locale,
                  decomposition);
        }
        break;
      case "remote":
        indexEngine = new ORemoteIndexEngine(indexId, name);
        break;
      default:
        throw new OIndexException("Unsupported storage type: " + storageType);
    }

    return indexEngine;
  }
}
