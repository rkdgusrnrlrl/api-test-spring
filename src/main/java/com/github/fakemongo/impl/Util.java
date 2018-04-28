package com.github.fakemongo.impl;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.LazyDBList;
import com.mongodb.gridfs.GridFSFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BSON;
import org.bson.LazyBSONObject;
import org.bson.types.Binary;

public final class Util {

  private Util() {
  }

  public static <T> BasicDBList list(T... ts) {
    return wrap(Arrays.asList(ts));
  }

  /**
   * Can extract field from an object.
   * Handle "field1.field2" in { field1 : {field2 : value2 } }
   *
   * @param object
   * @param field
   * @return null if not found.
   */
  public static <T> T extractField(DBObject object, String field) {
    if (object == null) {
      return null; // NPE ?
    }
    T value;
    int indexDot = field.indexOf('.');
    if (indexDot > 0) {
      String subField = field.substring(indexDot + 1);
      value = extractField(ExpressionParser.toDbObject(object.get(field.substring(0, indexDot))), subField);
    } else {
      value = (T) object.get(field);
    }
    return value;
  }

  /**
   * Can extract field from an object.
   * Handle "field1.field2" in { field1 : {field2 : value2 } }
   *
   * @return null if not found.
   */
  public static <T> T extractField(DBObject object, List<String> paths) {
    if (object == null) {
      return null; // NPE ?
    }
    DBObject value = object;
    for (String path : paths) {
      value = ExpressionParser.toDbObject(value.get(path));
      if (value == null) {
        break;
      }
    }
    return (T) value;
  }

  /**
   * Say "true" if field is in this object.
   * Handle "field1.field2" in { field1 : {field2 : value2 } }
   *
   * @param object
   * @param field
   * @return true if object contains field.
   */
  public static boolean containsField(DBObject object, String field) {
    if (object == null) {
      return false;
    }
    boolean result;
    int indexDot = field.indexOf('.');
    if (indexDot > 0) {
      String subField = field.substring(indexDot + 1);
      String actualField = field.substring(0, indexDot);
      result = false;
      if (object.containsField(actualField)) {
        Object value = object.get(actualField);
        if (ExpressionParser.isDbObject(value)) {
          result = containsField(ExpressionParser.toDbObject(value), subField);
        }
      }
    } else {
      result = object.containsField(field);
    }
    return result;
  }

  /**
   * Remove field in this object.
   *
   * @param object object to modify
   * @param field  field name, possibly with dot '.' to match hierarchy.
   */
  public static void removeField(DBObject object, String field) {
    if (object == null) {
      return;
    }
    int indexDot = field.indexOf('.');
    if (indexDot > 0) {
      String subField = field.substring(indexDot + 1);
      String actualField = field.substring(0, indexDot);
      if (object.containsField(actualField)) {
        Object value = object.get(actualField);
        if (ExpressionParser.isDbObject(value)) {
          removeField(ExpressionParser.toDbObject(value), subField);
        }
      }
    } else {
      object.removeField(field);
    }
  }

  /**
   * Put a value in a {@link DBObject} with hierarchy.
   *
   * @param dbObject object to modify
   * @param path     field with dot '.' to match hierarchy.
   * @param value    new value to set.
   */
  public static void putValue(DBObject dbObject, String path, Object value) {
    if (dbObject == null) {
      return; // NPE ?
    }
    int indexDot = path.indexOf('.');
    if (indexDot > 0) {
      String field = path.substring(0, indexDot);
      String nextPath = path.substring(indexDot + 1);

      // Create DBObject if necessary
      if (!dbObject.containsField(field)) {
        dbObject.put(field, new BasicDBObject());
      }
      putValue(ExpressionParser.toDbObject(dbObject.get(field)), nextPath, value);
    } else {
      dbObject.put(path, value);
    }
  }

  public static BasicDBList wrap(List otherList) {
    BasicDBList list = new BasicDBList();
    list.addAll(otherList);
    return list;
  }

  public static List<String> split(String key) {
    char dot = '.';
    int index = key.indexOf(dot);
    if (index <= 0) {
      return Collections.singletonList(key);
    } else {
      ArrayList<String> path = new ArrayList<String>(5);
      while (index > 0) {
        path.add(key.substring(0, index));
        key = key.substring(index + 1);
        index = key.indexOf(dot);
      }
      path.add(key);
      return path;
    }
  }

  public static boolean isPositiveInt(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  public static int compareToNullable(String s1, String s2) {
    if (s1 == null) {
      if (s2 == null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (s2 == null) {
        return 1;
      } else {
        return s1.compareTo(s2);
      }
    }
  }

  /**
   * @see org.bson.BasicBSONEncoder#_putObjectField(String, Object)
   */
  public static Object clone(Object source) {
    source = BSON.applyEncodingHooks(source);

    if (source instanceof UUID) {
      return source;
    }
    if (ExpressionParser.isDbObject(source)) {
      return clone(ExpressionParser.toDbObject(source));
    }
    if (source instanceof Binary) {
      return ((Binary) source).getData().clone();
    }
    if (source instanceof Date) {
      return new Date(((Date) source).getTime());
    }
    if (source instanceof Character) {
      return source.toString();
    }
    if (source instanceof Number) {
      Number n = (Number) source;
      if (n instanceof Integer ||
          n instanceof Short ||
          n instanceof Byte ||
          n instanceof AtomicInteger) {
        return n.intValue();
      } else if (n instanceof Long || n instanceof AtomicLong) {
        return n.longValue();
      } else if (n instanceof Float || n instanceof Double) {
        return n.doubleValue();
      } else {
        throw new IllegalArgumentException("can't serialize " + n.getClass());
      }
    }
    if (source instanceof byte[]) {
      return makeCopy((byte[]) source);
    }
//    }
//    if(source instanceof Cloneable) {
//      return ((Cloneable) source).clone();
//    }
    return source;
  }

  private static byte[] makeCopy(byte[] source) {
    if (source == null) {
      return null;
    }
    final byte[] copy = new byte[source.length];
    System.arraycopy(source, 0, copy, 0, source.length);
    return copy;
  }

  public static <T extends DBObject> T clone(T source) {
    if (source == null) {
      return null;
    }

    if (source instanceof BasicDBList) {
      @SuppressWarnings("unchecked")
      T clone = (T) ((BasicDBList) source).copy();
      return clone;
    }

    if (source instanceof org.bson.LazyBSONList) {
      BasicDBList clone = new BasicDBList();
      for (Object o : ((org.bson.LazyBSONList) source)) {
        if (o instanceof DBObject) {
          clone.add(clone(ExpressionParser.toDbObject(o)));
        } else {
          clone.add(o);
        }
      }
      return (T) clone;
    } else if (source instanceof LazyDBList) {
      BasicDBList clone = new BasicDBList();
      for (Object o : ((LazyDBList) source)) {
        if (ExpressionParser.isDbObject(o)) {
          clone.add(clone(ExpressionParser.toDbObject(o)));
        } else {
          clone.add(o);
        }
      }
      return (T) clone;
    }

    if (source instanceof BasicDBObject) {
      @SuppressWarnings("unchecked")
      T clone = (T) ((BasicDBObject) source).copy();
      return clone;
    }

    if (source instanceof LazyBSONObject) {
      @SuppressWarnings("unchecked")
      BasicDBObject clone = new BasicDBObject();
      for (Map.Entry<String, Object> entry : ((LazyBSONObject) source).entrySet()) {
        if (ExpressionParser.isDbObject(entry.getValue())) {
          clone.put(entry.getKey(), clone(ExpressionParser.toDbObject(entry.getValue())));
        } else {
          if (entry.getValue() instanceof Binary) {
            clone.put(entry.getKey(), ((Binary) entry.getValue()).getData().clone());
          } else {
            clone.put(entry.getKey(), entry.getValue());
          }
        }
      }
      return (T) clone;
    }

    @SuppressWarnings("unchecked")
    BasicDBObject clone = new BasicDBObject();
    for (Map.Entry<String, Object> entry : entrySet(source)) {
      if (ExpressionParser.isDbObject(entry.getValue())) {
        clone.put(entry.getKey(), clone(ExpressionParser.toDbObject(entry.getValue())));
      } else {
        if (entry.getValue() instanceof Binary) {
          clone.put(entry.getKey(), ((Binary) entry.getValue()).getData().clone());
        } else {
          clone.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return (T) clone;
  }

  @SuppressWarnings("unchecked")
  public static Set<Map.Entry<String, Object>> entrySet(DBObject object) {
    return (Set<Map.Entry<String, Object>>) object.toMap().entrySet();
  }

  /**
   * When inserting, MongoDB set _id in first place.
   *
   * @param source source to deep clone, can be null.
   * @return a cloned version of source, with _id field in first.
   */
  public static DBObject cloneIdFirst(DBObject source) {
    if (source == null) {
      return null;
    }

    // copy field values into new object
    DBObject newobj = new BasicDBObject();
    if (source.containsField(FongoDBCollection.ID_FIELD_NAME)) {
      newobj.put(FongoDBCollection.ID_FIELD_NAME, source.get(FongoDBCollection.ID_FIELD_NAME));
    }

    Set<Map.Entry<String, Object>> entrySet;
    if (source instanceof LazyBSONObject) {
      entrySet = ((LazyBSONObject) source).entrySet();
    } else if (source instanceof GridFSFile) {
      // GridFSFile.toMap doen't work.
      Map<String, Object> copyMap = new HashMap<String, Object>();
      for (String field : source.keySet()) {
        copyMap.put(field, source.get(field));
      }
      entrySet = copyMap.entrySet();
    } else {
      entrySet = source.toMap().entrySet();
    }
    // need to embedded the sub obj
    for (Map.Entry<String, Object> entry : entrySet) {
      String field = entry.getKey();
      if (!FongoDBCollection.ID_FIELD_NAME.equals(field)) {
        Object val = entry.getValue();
        if (ExpressionParser.isDbObject(val)) {
          newobj.put(field, clone(ExpressionParser.toDbObject(val)));
        } else {
          newobj.put(field, clone(val));
        }
      }
    }
    return newobj;
  }

  /**
   * @return true if the dbObject is empty.
   */
  public static boolean isDBObjectEmpty(DBObject projection) {
    return projection == null || projection.keySet().isEmpty();
  }

  public static Collection toCollection(Object array) {
    int length = Array.getLength(array);
    List list = new ArrayList();
    for (int i = 0; i < length; i++) {
      list.add(Array.get(array, i));
    }
    return list;
  }

  public static List toList(Collection expression) {
    if (expression instanceof List) {
      return (List) expression;
    }
    List list = new ArrayList();
    for (Object object : expression) {
      list.add(object);
    }
    return list;
  }

  public static Number genericAdd(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() + right.doubleValue();
    } else if (left instanceof Integer && right instanceof Integer) {
      return left.intValue() + right.intValue();
    } else {
      return left.longValue() + right.longValue();
    }
  }

  public static Number genericSub(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() - right.doubleValue();
    } else if (left instanceof Integer && right instanceof Integer) {
      return left.intValue() - right.intValue();
    } else {
      return left.longValue() - right.longValue();
    }
  }

  // http://docs.mongodb.org/manual/faq/developers/#faq-developers-multiplication-type-conversion
  public static Number genericMul(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() * (right.doubleValue());
    } else if (left instanceof Integer && right instanceof Integer) {
      return left.intValue() * right.intValue();
    } else {
      return left.longValue() * right.longValue();
    }
  }

  public static Number genericDiv(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() / (right.doubleValue());
    } else if (left instanceof Integer && right instanceof Integer) {
      return left.intValue() / right.intValue();
    } else {
      return left.longValue() / right.longValue();
    }
  }

  public static Number genericMod(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() % (right.doubleValue());
    } else if (left instanceof Integer && right instanceof Integer) {
      return left.intValue() % right.intValue();
    } else {
      return left.longValue() % right.longValue();
    }
  }

  public static Number genericMax(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return Math.max(left.doubleValue(), right.doubleValue());
    } else if (left instanceof Integer && right instanceof Integer) {
      return Math.max(left.intValue(), right.intValue());
    } else {
      return Math.max(left.longValue(), right.longValue());
    }
  }

  public static Date genericMax(Date left, Date right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return left.after(right) ? left : right;
  }

  public static Number genericMin(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return Math.min(left.doubleValue(), right.doubleValue());
    } else if (left instanceof Integer && right instanceof Integer) {
      return Math.min(left.intValue(), right.intValue());
    } else {
      return Math.min(left.longValue(), right.longValue());
    }
  }

  public static Date genericMin(Date left, Date right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return left.before(right) ? left : right;
  }
}
