package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.IndexInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Mapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author LomakiA <a href="mailto:a.lomakin@orientechnologies.com">Andrey Lomakin</a>
 * @since 21.12.11
 */
@Test(groups = {"index"})
public class MapIndexTest extends ObjectDBBaseTest {

  @Parameters(value = "url")
  public MapIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");

    final OClass mapper = database.getMetadata().getSchema().getClass("Mapper");
    mapper.createProperty("id", OType.STRING);
    mapper.createProperty("intMap", OType.EMBEDDEDMAP, OType.INTEGER);

    mapper.createIndex("mapIndexTestKey", OClass.INDEX_TYPE.NOTUNIQUE, "intMap");
    mapper.createIndex("mapIndexTestValue", OClass.INDEX_TYPE.NOTUNIQUE, "intMap by value");

    final OClass movie = database.getMetadata().getSchema().createClass("MapIndexTestMovie");
    movie.createProperty("title", OType.STRING);
    movie.createProperty("thumbs", OType.EMBEDDEDMAP, OType.INTEGER);

    movie.createIndex("indexForMap", OClass.INDEX_TYPE.NOTUNIQUE, "thumbs by key");
  }

  @AfterClass
  public void destroySchema() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().dropClass("Mapper");
    database.getMetadata().getSchema().dropClass("MapIndexTestMovie");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from Mapper")).execute();
    database.command(new OCommandSQL("delete from MapIndexTestMovie")).execute();
    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    final Mapper mapper = new Mapper();
    final Map<String, Integer> map = new HashMap<>();
    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    database.save(mapper);

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    try {
      database.begin();
      final Mapper mapper = new Mapper();
      Map<String, Integer> map = new HashMap<>();

      map.put("key1", 10);
      map.put("key2", 20);

      mapper.setIntMap(map);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setIntMap(mapTwo);
    database.save(mapper);

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getInternal().size(), 2);

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    database.begin();
    try {
      final Map<String, Integer> mapTwo = new HashMap<>();

      mapTwo.put("key3", 30);
      mapTwo.put("key2", 20);

      mapper.setIntMap(mapTwo);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    database.begin();
    final Map<String, Integer> mapTwo = new HashMap<>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setIntMap(mapTwo);
    database.save(mapper);
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database
        .command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key3', 30"))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().put("key3", 30);
      database.save(loadedMapper);

      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().put("key3", 30);
    database.save(loadedMapper);
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database
        .command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key2', 40"))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(40).findAny().isPresent());
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().put("key2", 40);
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(40).findAny().isPresent());
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().put("key2", 40);
    database.save(loadedMapper);
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database
        .command(new OCommandSQL("UPDATE " + mapper.getId() + " remove intMap = 'key2'"))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().remove("key2");
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().remove("key2");
    database.save(loadedMapper);
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key3").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(30).findAny().isPresent());
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);
    database.delete(mapper);

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getInternal().size(), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      database.delete(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getInternal().size(), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getInternal().size(), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    database.delete(mapper);
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final IndexInternal indexKeyInternal = keyIndexMap.getInternal();

    Assert.assertTrue(indexKeyInternal.getRids("key1").findAny().isPresent());
    Assert.assertTrue(indexKeyInternal.getRids("key2").findAny().isPresent());

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final IndexInternal indexValueInternal = valueIndexMap.getInternal();

    Assert.assertTrue(indexValueInternal.getRids(10).findAny().isPresent());
    Assert.assertTrue(indexValueInternal.getRids(20).findAny().isPresent());
  }

  public void testIndexMapSQL() {
    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    database.save(mapper);

    final List<Mapper> resultByKey =
        database.query(
            new OSQLSynchQuery<Mapper>("select * from Mapper where intMap containskey ?"), "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    Assert.assertEquals(map, resultByKey.get(0).getIntMap());

    final List<Mapper> resultByValue =
        database.query(
            new OSQLSynchQuery<Mapper>("select * from Mapper where intMap containsvalue ?"), 10);
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, resultByValue.get(0).getIntMap());
  }
}
