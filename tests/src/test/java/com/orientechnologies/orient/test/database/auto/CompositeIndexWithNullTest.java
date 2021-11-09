package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/11/14
 */
public class CompositeIndexWithNullTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public CompositeIndexWithNullTest(@Optional String url) {
    super(url);
  }

  public void testPointQueryInTx() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryInTxClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "compositeIndexNullPointQueryInTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      final ODocument document = new ODocument("compositeIndexNullPointQueryInTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) {
        document.field("prop3", i);
      }

      document.save();
    }

    database.commit();

    String query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 5; k++) {
        OElement document = result.next().toElement();
        Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
        Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
      }

      Assert.assertFalse(result.hasNext());
    }

    query =
        "select from compositeIndexNullPointQueryInTxClass where prop1 = 1 and prop2 = 2 and prop3 is null";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 2; k++) {
        OElement document = result.next().toElement();
        Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
        Assert.assertEquals(document.<Object>getProperty("prop2"), 2);
        Assert.assertNull(document.getProperty("prop3"));
      }

      Assert.assertFalse(result.hasNext());
    }
  }

  public void testPointQueryInMiddleTx() {
    if (database.getURL().startsWith("remote:")) return;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryInMiddleTxClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "compositeIndexNullPointQueryInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) document.field("prop3", i);

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 5; k++) {
        final OElement element = result.next().toElement();
        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
        Assert.assertEquals(element.<Object>getProperty("prop2"), 2);
      }

      Assert.assertFalse(result.hasNext());
    }

    query =
        "select from compositeIndexNullPointQueryInMiddleTxClass where prop1 = 1 and prop2 = 2 and prop3 is null";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 2; k++) {
        final OElement element = result.next().toElement();
        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
        Assert.assertEquals(element.<Object>getProperty("prop2"), 2);
        Assert.assertNull(element.<Object>getProperty("prop3"));
      }

      Assert.assertFalse(result.hasNext());
    }

    database.commit();
  }

  public void testRangeQueryInMiddleTx() {
    if (database.getURL().startsWith("remote:")) return;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryInMiddleTxClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "compositeIndexNullRangeQueryInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null,
        new String[] {"prop1", "prop2", "prop3"});

    database.begin();
    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryInMiddleTxClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0) document.field("prop3", i);

      document.save();
    }

    String query =
        "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 = 1 and prop2 > 2";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 5; k++) {
        final OElement element = result.next().toElement();

        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
        Assert.assertTrue(element.<Integer>getProperty("prop2") > 2);
      }

      Assert.assertFalse(result.hasNext());
    }

    query = "select from compositeIndexNullRangeQueryInMiddleTxClass where prop1 > 0";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 10; k++) {
        final OElement element = result.next().toElement();

        Assert.assertTrue(element.<Integer>getProperty("prop1") > 0);
      }

      Assert.assertFalse(result.hasNext());
    }

    database.commit();
  }

  public void testPointQueryNullInTheMiddleInMiddleTx() {
    if (database.getURL().startsWith("remote:")) return;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "compositeIndexNullPointQueryNullInTheMiddleInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        null,
        new String[] {"prop1", "prop2", "prop3"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      ODocument document =
          new ODocument("compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) document.field("prop2", i);

      document.field("prop3", i);

      document.save();
    }

    String query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1";
    try (final OResultSet result = database.query(query)) {
      for (int k = 0; k < 10; k++) {
        final OElement element = result.next().toElement();
        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
      }

      Assert.assertFalse(result.hasNext());
    }

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and prop2 is null";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 5; k++) {
        final OElement element = result.next().toElement();
        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
        Assert.assertNull(element.getProperty("prop2"));
      }

      Assert.assertFalse(result.hasNext());
    }

    query =
        "select from compositeIndexNullPointQueryNullInTheMiddleInMiddleTxClass where prop1 = 1 and prop2 is null and prop3 = 13";
    try (OResultSet result = database.query(query)) {
      for (int k = 0; k < 1; k++) {
        final OElement element = result.next().toElement();
        Assert.assertEquals(element.<Object>getProperty("prop1"), 1);
        Assert.assertEquals(element.<Object>getProperty("prop3"), 13);
        Assert.assertNull(element.getProperty("prop2"));
      }

      Assert.assertFalse(result.hasNext());
    }

    database.commit();
  }

  public void testRangeQueryNullInTheMiddleInMiddleTx() {
    if (database.getURL().startsWith("remote:")) return;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxIndex",
        OClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"prop1", "prop2", "prop3"});

    for (int i = 0; i < 20; i++) {
      ODocument document =
          new ODocument("compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0) document.field("prop2", i);

      document.field("prop3", i);

      document.save();
    }

    final String query =
        "select from compositeIndexNullRangeQueryNullInTheMiddleInMiddleTxClass where prop1 > 0";
    try (final OResultSet result = database.query(query)) {
      for (int k = 0; k < 10; k++) {
        OElement document = result.next().toElement();
        Assert.assertEquals(document.<Object>getProperty("prop1"), 1);
      }

      Assert.assertFalse(result.hasNext());
    }
  }
}
