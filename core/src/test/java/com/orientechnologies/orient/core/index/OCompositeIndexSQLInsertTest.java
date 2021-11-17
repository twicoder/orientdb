package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OCompositeIndexSQLInsertTest {

  public ODatabaseDocument db;

  @Before
  public void before() {
    System.err.println("BEFORE THREAD: " + Thread.currentThread());
    db = new ODatabaseDocumentTx("memory:" + OCompositeIndexSQLInsertTest.class.getSimpleName());
    db.create();
    OSchema schema = db.getMetadata().getSchema();
    OClass book = schema.createClass("Book");
    book.createProperty("author", OType.STRING);
    book.createProperty("title", OType.STRING);
    book.createProperty("publicationYears", OType.EMBEDDEDLIST, OType.INTEGER);
    book.createIndex("books", "unique", "author", "title", "publicationYears");

    book.createProperty("nullKey1", OType.STRING);
    ODocument indexOptions = new ODocument();
    indexOptions.field("ignoreNullValues", true);
    book.createIndex(
        "indexignoresnulls", "NOTUNIQUE", null, indexOptions, new String[] {"nullKey1"});
  }

  @After
  public void after() {
    System.err.println("AFTER THREAD: " + Thread.currentThread());
    db.activateOnCurrentThread();
    db.drop();
  }

  @Test
  public void testCompositeIndexWithRangeAndContains() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexWithRangeAndConditions");
    clazz.createProperty("id", OType.INTEGER);
    clazz.createProperty("bar", OType.INTEGER);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    clazz.createProperty("name", OType.STRING);

    db.command(
            new OCommandSQL(
                "create index CompositeIndexWithRangeAndConditions_id_tags_name on CompositeIndexWithRangeAndConditions (id, tags, name) NOTUNIQUE"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"green\",\"yellow\"] , name = \"Foo\", bar = 1"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"blue\",\"black\"] , name = \"Foo\", bar = 14"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"white\"] , name = \"Foo\""))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"green\",\"yellow\"], name = \"Foo1\", bar = 14"))
        .execute();

    final OResultSet r =
        db.query("select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1");
    Assert.assertEquals(1, r.stream().count());

    final OResultSet r1 =
        db.query(
            "select from CompositeIndexWithRangeAndConditions where id = 1 and tags CONTAINS \"white\"");
    Assert.assertEquals(r1.stream().count(), 1);

    final OResultSet r2 =
        db.query(
            "select from CompositeIndexWithRangeAndConditions where id > 0 and tags CONTAINS \"white\"");
    Assert.assertEquals(r2.stream().count(), 1);

    final OResultSet r3 =
        db.query("select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1");

    Assert.assertEquals(r3.stream().count(), 1);

    final OResultSet r4 =
        db.query(
            "select from CompositeIndexWithRangeAndConditions where tags CONTAINS \"white\" and id > 0");
    Assert.assertEquals(r4.stream().count(), 1);
  }
}
