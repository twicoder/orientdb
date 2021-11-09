package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OChainedIndexProxy;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Testing functionality of {@link OChainedIndexProxy}.
 *
 * <p>Each test method tests different traverse index combination with different operations.
 *
 * <p>Method name are used to describe test case, first part is chain of types of indexes that are
 * used in test, second part define operation which are tested, and the last part describe whether
 * or not {@code limit} operator are used in query.
 *
 * <p>
 *
 * <p>Prefix "lpirt" in class names means "LinkedPropertyIndexReuseTest".
 */
@SuppressWarnings("SuspiciousMethodCalls")
@Test(groups = {"index"})
public class SQLSelectByLinkedPropertyIndexReuseTest extends AbstractIndexReuseTest {

  @Parameters(value = "url")
  public SQLSelectByLinkedPropertyIndexReuseTest(@Optional final String iURL) {
    super(iURL);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    if (database.isClosed()) {
      database.open("admin", "admin");
    }

    createSchemaForTest();
    fillDataSet();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database.open("admin", "admin");
    }

    database.command(new OCommandSQL("drop class lpirtStudent")).execute();
    database.command(new OCommandSQL("drop class lpirtGroup")).execute();
    database.command(new OCommandSQL("drop class lpirtCurator")).execute();
    database.getMetadata().getSchema().reload();

    database.close();
    super.afterClass();
  }

  @Test
  public void testHashIndexIsUsedAsBaseIndex() {
    long oldIndexUsage = indexUsages();

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<ODocument>("select from lpirtStudent where transcript.id = '1'"));

    assertEquals(result.size(), 1);

    assertEquals(indexUsages(), oldIndexUsage + 2);
  }

  @Test
  public void testCompositeHashIndexIgnored() {
    long oldIndexUsage = indexUsages();

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<ODocument>("select from lpirtStudent where skill.name = 'math'"));

    assertEquals(result.size(), 1);

    assertEquals(indexUsages(), oldIndexUsage);
  }

  private long indexUsages() {
    final long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    return oldIndexUsage == -1 ? 0 : oldIndexUsage;
  }

  /** William James and James Bell work together on the same diploma. */
  private void fillDataSet() {
    ODocument curator1 = database.newInstance("lpirtCurator");
    curator1.field("name", "Someone");
    curator1.field("salary", 2000);

    final ODocument group1 = database.newInstance("lpirtGroup");
    group1.field("name", "PZ-08-1");
    group1.field("curator", curator1);
    group1.save();

    final ODocument diploma1 = database.newInstance("lpirtDiploma");
    diploma1.field("GPA", 3.);
    diploma1.field("name", "diploma1");
    diploma1.field(
        "thesis",
        "Researching and visiting universities before making a final decision is very "
            + "beneficial because you student be able to experience the campus, meet the professors, and truly "
            + "understand the traditions of the university.");

    final ODocument transcript = database.newInstance("lpirtTranscript");
    transcript.field("id", "1");

    final ODocument skill = database.newInstance("lpirtSkill");
    skill.field("name", "math");

    final ODocument student1 = database.newInstance("lpirtStudent");
    student1.field("name", "John Smith");
    student1.field("group", group1);
    student1.field("diploma", diploma1);
    student1.field("transcript", transcript);
    student1.field("skill", skill);
    student1.save();

    ODocument curator2 = database.newInstance("lpirtCurator");
    curator2.field("name", "Someone else");
    curator2.field("salary", 500);

    final ODocument group2 = database.newInstance("lpirtGroup");
    group2.field("name", "PZ-08-2");
    group2.field("curator", curator2);
    group2.save();

    final ODocument diploma2 = database.newInstance("lpirtDiploma");
    diploma2.field("GPA", 5.);
    diploma2.field("name", "diploma2");
    diploma2.field(
        "thesis",
        "While both Northerners and Southerners believed they fought against tyranny and "
            + "oppression, Northerners focused on the oppression of slaves while Southerners defended their own "
            + "right to self-government.");

    final ODocument student2 = database.newInstance("lpirtStudent");
    student2.field("name", "Jane Smith");
    student2.field("group", group2);
    student2.field("diploma", diploma2);
    student2.save();

    ODocument curator3 = database.newInstance("lpirtCurator");
    curator3.field("name", "Someone else");
    curator3.field("salary", 600);

    final ODocument group3 = database.newInstance("lpirtGroup");
    group3.field("name", "PZ-08-3");
    group3.field("curator", curator3);
    group3.save();

    final ODocument diploma3 = database.newInstance("lpirtDiploma");
    diploma3.field("GPA", 4.);
    diploma3.field("name", "diploma3");
    diploma3.field(
        "thesis",
        "College student shouldn't have to take a required core curriculum, and many core "
            + "courses are graded too stiffly.");

    final ODocument student3 = database.newInstance("lpirtStudent");
    student3.field("name", "James Bell");
    student3.field("group", group3);
    student3.field("diploma", diploma3);
    student3.save();

    final ODocument student4 = database.newInstance("lpirtStudent");
    student4.field("name", "Roger Connor");
    student4.field("group", group3);
    student4.save();

    final ODocument student5 = database.newInstance("lpirtStudent");
    student5.field("name", "William James");
    student5.field("group", group3);
    student5.field("diploma", diploma3);
    student5.save();
  }

  private void createSchemaForTest() {
    final OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("lpirtStudent")) {
      final OClass curatorClass = schema.createClass("lpirtCurator");
      curatorClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      curatorClass
          .createProperty("salary", OType.INTEGER)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
      curatorClass.createIndex(
          "curotorCompositeIndex",
          OClass.INDEX_TYPE.UNIQUE.name(),
          null,
          new ODocument().field("ignoreNullValues", true),
          new String[] {"salary", "name"});

      final OClass groupClass = schema.createClass("lpirtGroup");
      groupClass
          .createProperty("name", OType.STRING)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
      groupClass
          .createProperty("curator", OType.LINK, curatorClass)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));

      final OClass diplomaClass = schema.createClass("lpirtDiploma");
      diplomaClass.createProperty("GPA", OType.DOUBLE).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      diplomaClass.createProperty("thesis", OType.STRING);
      diplomaClass
          .createProperty("name", OType.STRING)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
      diplomaClass.createIndex(
          "diplomaThesisUnique",
          OClass.INDEX_TYPE.UNIQUE.name(),
          null,
          new ODocument().field("ignoreNullValues", true),
          new String[] {"thesis"});

      final OClass transcriptClass = schema.createClass("lpirtTranscript");
      transcriptClass
          .createProperty("id", OType.STRING)
          .createIndex(
              OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, new ODocument().field("ignoreNullValues", true));

      final OClass skillClass = schema.createClass("lpirtSkill");
      skillClass
          .createProperty("name", OType.STRING)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));

      final OClass studentClass = schema.createClass("lpirtStudent");
      studentClass
          .createProperty("name", OType.STRING)
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
      studentClass
          .createProperty("group", OType.LINK, groupClass)
          .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      studentClass.createProperty("diploma", OType.LINK, diplomaClass);
      studentClass
          .createProperty("transcript", OType.LINK, transcriptClass)
          .createIndex(
              OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, new ODocument().field("ignoreNullValues", true));
      studentClass.createProperty("skill", OType.LINK, skillClass);

      final ODocument metadata = new ODocument().field("ignoreNullValues", false);
      studentClass.createIndex(
          "studentDiplomaAndNameIndex",
          OClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          metadata.copy(),
          new String[] {"diploma", "name"});
      studentClass.createIndex(
          "studentSkillAndGroupIndex",
          OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString(),
          null,
          metadata.copy(),
          new String[] {"skill", "group"});
    }
  }

  private int containsDocumentWithFieldValue(
      final List<ODocument> docList, final String fieldName, final Object fieldValue) {
    int count = 0;
    for (final ODocument docItem : docList) {
      if (fieldValue.equals(docItem.field(fieldName))) {
        count++;
      }
    }
    return count;
  }
}
