package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareMultiValueGetValuesTest extends DocumentDBBaseTest {
  private static final String CLASS_NAME = "idxTxAwareMultiValueGetValuesTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX = "idxTxAwareMultiValueGetValuesTestIndex";

  @Parameters(value = "url")
  public IndexTxAwareMultiValueGetValuesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(PROPERTY_NAME, OType.INTEGER);
    cls.createIndex(INDEX, OClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultTwo.size(), 4);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    ODocument documentOne = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    ODocument documentTwo = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    documentOne.delete();
    documentTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemoveOne() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    ODocument documentOne = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    documentOne.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testMultiPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    final ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();

    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 2);

    database.commit();

    result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 2);
    database.commit();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();

    result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 1);

    database.commit();

    result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 1);

    database.commit();

    result = entrySet(index.getInternal(), Arrays.asList(1, 2));

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(
        index instanceof IndexTxAwareMultiValueOriginalKey
            || index instanceof IndexTxAwareMultiValueBinaryKey);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = entrySet(index.getInternal(), Arrays.asList(1, 2));
    Assert.assertEquals(result.size(), 2);

    database.commit();

    result = entrySet(index.getInternal(), Arrays.asList(1, 2));

    Assert.assertEquals(result.size(), 2);
  }

  private static Set<OIdentifiable> entrySet(
      final IndexInternal indexInternal, final Collection<Integer> keys) {
    if (indexInternal instanceof IndexInternalOriginalKey) {
      final IndexInternalOriginalKey indexInternalOriginalKey =
          (IndexInternalOriginalKey) indexInternal;
      try (final Stream<ORawPair<Object, ORID>> stream =
          indexInternalOriginalKey.streamEntries(keys, true)) {
        return stream.map(pair -> pair.second).collect(Collectors.toSet());
      }
    } else {
      final IndexInternalBinaryKey indexInternalBinaryKey = (IndexInternalBinaryKey) indexInternal;
      try (final Stream<ORawPair<byte[], ORID>> stream =
          indexInternalBinaryKey.streamEntries(keys, true)) {
        return stream.map(pair -> pair.second).collect(Collectors.toSet());
      }
    }
  }
}
