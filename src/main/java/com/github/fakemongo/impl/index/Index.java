package com.github.fakemongo.impl.index;

import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An index for the MongoDB.
 */
public class Index extends IndexAbstract<DBObject> {

  Index(String name, DBObject keys, boolean unique, boolean sparse) {
    super(name, keys, unique, createMap(keys, unique), null, sparse);
  }

  private static Map<DBObject, IndexedList<DBObject>> createMap(DBObject keys, boolean unique) {
    // Preserve order only for id.
    if (keys.containsField(FongoDBCollection.ID_FIELD_NAME) && keys.toMap().size() == 1) {
      return new LinkedHashMap<DBObject, IndexedList<DBObject>>();
    } else {
      //noinspection unchecked
      return new TreeMap<DBObject, IndexedList<DBObject>>(new ExpressionParser().buildObjectComparator(isAsc(keys)));
    }
  }

  @Override
  public DBObject embedded(DBObject object) {
    return expandObject(object); // Important : do not clone, indexes share objects between them.
  }

  /**
   * Expand all flattened {@link DBObject}s to match the current MongoDB behaviour.
   *
   * @param object The {@link DBObject} to insert.
   * @return The expanded {@link DBObject}.
   */
  private DBObject expandObject(final DBObject object) {
    final List<String> keysToRemove = new ArrayList<String>();
    final List<DBObject> objectsToPut = new ArrayList<DBObject>();

    for (final String key : object.keySet()) {
      if (key.contains(".")) {
        final Object actualValue = object.get(key);

        DBObject expandedObject = null;

        final List<String> splittedKeys = Util.split(key);

        for (int i = splittedKeys.size() - 1; i >= 0; i--) {
          if (expandedObject == null) {
            expandedObject = new BasicDBObject(splittedKeys.get(i), actualValue);
          } else {
            final DBObject partialObject = expandedObject;
            expandedObject = new BasicDBObject(splittedKeys.get(i), partialObject);
          }
        }

        keysToRemove.add(key);
        objectsToPut.add(expandedObject);
      }
    }

    for (final String keyToRemove : keysToRemove) {
      object.removeField(keyToRemove);
    }

    for (final DBObject objectToPut : objectsToPut) {
      final String rootElement = objectToPut.keySet().iterator().next();
      if (object.containsField(rootElement)) {
        DBObject objectToAdd = ExpressionParser.toDbObject(objectToPut.get(rootElement));
        ExpressionParser.toDbObject(object.get(rootElement)).putAll(objectToAdd);
      } else {
        object.putAll(objectToPut);
      }
    }

    return object;
  }
}
