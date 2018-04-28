package com.github.fakemongo.impl.index;

import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Filter;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;

import static com.mongodb.FongoDBCollection.ID_FIELD_NAME;

import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.bson.types.Binary;

/**
 * An index for the MongoDB.
 * <p/>
 * NOT Thread Safe. The ThreadSafety must be done by the caller.
 */
public abstract class IndexAbstract<T extends DBObject> {
  final String geoIndex;
  final ExpressionParser expressionParser = new ExpressionParser();
  // Contains all dbObject than field value can have
  final Map<T, IndexedList<T>> mapValues;
  private final String name;
  private final DBObject keys;
  private final Set<String> fields;
  private final boolean unique;
  private final boolean sparse;
  int lookupCount = 0;

  IndexAbstract(String name, DBObject keys, boolean unique, Map<T, IndexedList<T>> mapValues, String geoIndex, boolean sparse) throws MongoException {
    this.name = name;
    this.fields = Collections.unmodifiableSet(keys.keySet()); // Setup BEFORE keys.
    this.keys = prepareKeys(keys);
    this.unique = unique;
    this.mapValues = mapValues;
    this.geoIndex = geoIndex;
    this.sparse = sparse;

    for (Object value : keys.toMap().values()) {
      if (!(value instanceof String) && !(value instanceof Number)) {
        //com.mongodb.WriteConcernException: { "serverUsed" : "/127.0.0.1:27017" , "err" : "bad index key pattern { a: { n: 1 } }" , "code" : 10098 , "n" : 0 , "connectionId" : 543 , "ok" : 1.0}
        throw new MongoException(67, "bad index key pattern : " + keys);
      }
    }
  }

  static boolean isAsc(DBObject keys) {
    Object value = keys.toMap().values().iterator().next();
    return value instanceof Number && ((Number) value).intValue() >= 1;
  }

  private DBObject prepareKeys(DBObject keys) {
    DBObject nKeys = Util.clone(keys);
    if (!nKeys.containsField(ID_FIELD_NAME)) {
      // Remove _id for projection.
      boolean exclude = true;
      // To be sure than ID_FIELD_NAME is not in a compbound index.
      for (String key : nKeys.keySet()) {
        if (key.startsWith(ID_FIELD_NAME)) {
          exclude = false;
          break;
        }
      }
      if (exclude) {
        nKeys.put("_id", 0);
      }
    }
    // Transform 2d indexes into "1" (for now, can change later).
    for (Map.Entry<String, Object> entry : Util.entrySet(keys)) { // Work on keys to avoid ConcurrentModificationException
      if (entry.getValue().equals("2d") || entry.getValue().equals("2dsphere")) {
        nKeys.put(entry.getKey(), 1);
      }
      if (entry.getValue() instanceof Number && ((Number) entry.getValue()).longValue() < 0) {
        nKeys.put(entry.getKey(), 1); // Cannot mix -1 / +1 in projection.
      }
    }
    return nKeys;
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  public boolean isSparse() {
    return sparse;
  }

  public boolean isGeoIndex() {
    return geoIndex != null;
  }

  public DBObject getKeys() {
    return keys;
  }

  public Set<String> getFields() {
    return fields;
  }

  /**
   * @param object    new object to insert in the index.
   * @param oldObject in update, old objet to remove from index.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> addOrUpdate(T object, T oldObject) {
    if (oldObject != null) {
      remove(oldObject);
    }

    T key = getKeyFor(object);
    // In a sparse index, we only add to the index if the full key is there.
    if (sparse && isPartialKey(key)) {
      return Collections.emptyList();
    }

    if (unique) {
      // Unique must check if he's really unique.
      if (mapValues.containsKey(key)) {
        return extractFields(object, key.keySet());
      }
      T toAdd = embedded(object);
      mapValues.put(key, new IndexedList<T>(Collections.singletonList(toAdd))); // DO NOT CLONE !
    } else {
      // Extract previous values
      IndexedList<T> values = mapValues.get(key);
      if (values == null) {
        // Create if absent.
        values = new IndexedList<T>(new ArrayList<T>());
        mapValues.put(key, values);
      }

      // Add to values.
      T toAdd = embedded(object); // DO NOT CLONE ! Indexes must share the same object.
      values.add(toAdd);
    }
    return Collections.emptyList();
  }

  private boolean isPartialKey(T key) {
    final Set<String> keyProjections = generateProjections(key, "");
    return !getFields().equals(keyProjections);
  }

  private Set<String> generateProjections(T object, final String parentPath) {
    final Set<String> rval = new TreeSet<String>();
    for (String objectKey : object.keySet()) {
      Object value = object.get(objectKey);
      if (value instanceof List) {
        List valueList = (List) value;
        for (Object listItem : valueList) {
          if (listItem instanceof DBObject) {
            rval.addAll(generateProjections((T) listItem, parentPath + objectKey + "."));
          } else {
            rval.add(parentPath + objectKey);
          }
        }
      } else if (value instanceof DBObject) {
        rval.addAll(generateProjections((T) value, parentPath + objectKey + "."));
      } else {
        rval.add(parentPath + objectKey);
      }
    }
    return rval;
  }

  public abstract T embedded(DBObject object);

  /**
   * Check, in case of unique index, if we can add it.
   *
   * @param object
   * @param oldObject old object if update, null elsewhere.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> checkAddOrUpdate(T object, T oldObject) {
    if (unique) {
      DBObject key = getKeyFor(object);
      IndexedList<T> objects = mapValues.get(key);
      if (objects != null && !objects.contains(oldObject)) {
        List<List<Object>> fieldsForIndex = extractFields(object, getFields());
        return fieldsForIndex;
      }
    }
    return Collections.emptyList();
  }

  /**
   * Remove an object from the index.
   *
   * @param object to remove from the index.
   */
  public void remove(T object) {
    T key = getKeyFor(object);
    // Extract previous values
    IndexedList<T> values = mapValues.get(key);
    if (values != null) {
      // Last entry ? or uniqueness ?
      if (values.size() == 1) {
        mapValues.remove(key);
      } else {
        values.remove(object);
      }
    }
  }

  /**
   * Multiple add of objects.
   *
   * @param objects to add.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> addAll(Iterable<T> objects) {
    for (T object : objects) {
      if (canHandle(object)) {
        List<List<Object>> nonUnique = addOrUpdate(object, null);
        // TODO(twillouer) : must handle writeConcern.
        if (!nonUnique.isEmpty()) {
          return nonUnique;
        }
      }
    }
    return Collections.emptyList();
  }

  // Only for unique index and for query with values. ($in doesn't work by example.)
  public List<T> get(DBObject query) {
    if (!unique) {
      throw new IllegalStateException("get is only for unique index");
    }
    lookupCount++;

    T key = getKeyFor(query);
    IndexedList<T> result = mapValues.get(key);
    if (result != null)
      return result.getElements();
    else
      return null;
  }

  // @Nonnull
  public Collection<T> retrieveObjects(DBObject query) {
    // Optimization
    if (unique && query.keySet().size() == 1) {
      Object key = query.toMap().values().iterator().next();
      if (!(ExpressionParser.isDbObject(key) || key instanceof Binary || key instanceof byte[])) {
        List<T> result = get(query);
        if (result != null) {
          return result;
        }
      }
    }

    lookupCount++;

    // Filter for the key.
    Filter filterKey = expressionParser.buildFilter(query, getFields());
    // Filter for the data.
    Filter filter = expressionParser.buildFilter(query);
    List<T> result = new ArrayList<T>();
    for (Map.Entry<T, IndexedList<T>> entry : mapValues.entrySet()) {
      if (filterKey.apply(entry.getKey())) {
        for (T object : entry.getValue().getElements()) {
          if (filter.apply(object)) {
            result.add(object); // DO NOT CLONE ! need for update.
          }
        }
      }
    }
    return result;
  }

  public long getLookupCount() {
    return lookupCount;
  }

  public int size() {
    int size = 0;
    if (unique) {
      size = mapValues.size();
    } else {
      for (Map.Entry<T, IndexedList<T>> entry : mapValues.entrySet()) {
        size += entry.getValue().size();
      }
    }
    return size;
  }

  public List<DBObject> values() {
    List<DBObject> values = new ArrayList<DBObject>(mapValues.size() * 10);
    for (IndexedList<T> objects : mapValues.values()) {
      values.addAll(objects.getElements());
    }
    return values;
  }

  public void clear() {
    mapValues.clear();
  }

  /**
   * Return true if index can handle this query.
   *
   * @param queryFields fields of the query.
   * @return true if index can be used.
   */
  public boolean canHandle(final DBObject queryFields) {
    if (queryFields == null) {
      return false;
    }

    //get keys including embedded indexes
    for (String field : fields) {
      final Object o = queryFields.get(field);
      if (o == null && !keyEmbeddedFieldMatch(field, queryFields)) {
        return false;
      }
      if (ExpressionParser.isDbObject(o) && ExpressionParser.toDbObject(o).containsField(QueryOperators.EXISTS)) {
        return false;
      }
    }
    return true;
  }

  private boolean keyEmbeddedFieldMatch(String field, DBObject queryFields) {
    //if field embedded field type
    String[] fieldParts = field.split("\\.");
    if (fieldParts.length == 0) {
      return false;
    }

    DBObject searchQueryFields = queryFields;
    int count = 0;
    for (String fieldPart : fieldParts) {
      count++;
      if (searchQueryFields instanceof BasicDBList) {
        // when it's a list, there's no need to investigate nested documents
        return true;
      } else if (!searchQueryFields.containsField(fieldPart) || searchQueryFields.get(fieldPart) == null) { // Change if sparse ?
        return false;
      } else if (ExpressionParser.isDbObject(searchQueryFields.get(fieldPart))) {
        searchQueryFields = ExpressionParser.toDbObject(searchQueryFields.get(fieldPart));
      }
    }

    return fieldParts.length == count;
  }

  @Override
  public String toString() {
    return "Index{" +
        "name='" + name + '\'' +
        '}';
  }

  /**
   * Create the key for the hashmap.
   * TODO: This is actually an invalid key model. If a field within a list is indexed, one document produces multiple keys
   */
  T getKeyFor(DBObject object) {
    DBObject applyProjections = FongoDBCollection.applyProjections(object, keys);
    return (T) pruneEmptyListObjects(applyProjections);
  }

  // Applying the projection may leave some empty objects within lists.
  // For example, if our full document is: { _id: 1, list: [ {foo: 7}, {foo: 8}, {bar: 6}, {baz: 3} ] }
  // Then a projection of { "list.foo": 1 } will result in: { list: [ {foo: 7}, {foo: 8}, {}, {} ] }
  // This poses a problem for unique indexes, because the same values for indexed fields can have 
  // different projections in the presence of list size variation. 
  private DBObject pruneEmptyListObjects(DBObject projectedObject) {
    BasicDBObject ret = new BasicDBObject();
    for (String projectionKey : projectedObject.keySet()) {
      final Object projectedValue = projectedObject.get(projectionKey);
      if (projectedValue instanceof List) {
        BasicDBList prunedList = pruneList((List) projectedValue);
        ret.put(projectionKey, prunedList);
      } else if (ExpressionParser.isDbObject(projectedValue)) {
        ret.put(projectionKey, pruneEmptyListObjects((DBObject) projectedValue));
      } else {
        ret.put(projectionKey, projectedValue);
      }
    }
    return ret;
  }

  private BasicDBList pruneList(List inList) {
    BasicDBList ret = new BasicDBList();

    for (Object listItem : inList) {
      if (listItem instanceof List) {
        ret.add((List) listItem);
      } else if (listItem instanceof DBObject){
        if (!((DBObject) listItem).keySet().isEmpty()) {
          ret.add(listItem);
        }
      } else {
        ret.add(listItem);
      }
    }
    return ret;
  }

  private List<List<Object>> extractFields(DBObject dbObject, Collection<String> fields) {
    List<List<Object>> fieldValue = new ArrayList<List<Object>>();
    for (String field : fields) {
      List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
      fieldValue.add(embeddedValues);
    }
    return fieldValue;
  }
}
