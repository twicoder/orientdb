package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.index.IndexInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class IndexTxTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexTxTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.createClass("IndexTxTestClass");
    cls.createProperty("name", OType.STRING);
    cls.createIndex("IndexTxTestIndex", OClass.INDEX_TYPE.UNIQUE, "name");
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.getClass("IndexTxTestClass");
    if (cls != null) {
      cls.truncate();
    }
  }

  @Test
  public void testIndexCrossReferencedDocuments() {
    checkEmbeddedDB();

    database.begin();

    final ODocument doc1 = new ODocument("IndexTxTestClass");
    final ODocument doc2 = new ODocument("IndexTxTestClass");

    doc1.save();
    doc2.save();

    doc1.field("ref", doc2.getIdentity().copy());
    doc1.field("name", "doc1");
    doc2.field("ref", doc1.getIdentity().copy());
    doc2.field("name", "doc2");

    doc1.save();
    doc2.save();

    database.commit();

    OIndex index = getIndex("IndexTxTestIndex");
    IndexInternal indexInternal = index.getInternal();
    Assert.assertEquals(2, indexInternal.size());

    Assert.assertEquals(
        indexInternal.getRids("doc1").findFirst().orElseThrow(AssertionError::new),
        doc1.getIdentity());
    Assert.assertEquals(
        indexInternal.getRids("doc2").findFirst().orElseThrow(AssertionError::new),
        doc2.getIdentity());
  }
}
