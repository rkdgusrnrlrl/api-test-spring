package com.github.fakemongo.impl;

import com.github.fakemongo.FongoException;
import com.github.fakemongo.impl.geo.GeoUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.FongoDBCollection;
import com.mongodb.LazyDBObject;
import com.mongodb.QueryOperators;
import com.mongodb.util.FongoJSON;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.bson.LazyBSONList;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;


@SuppressWarnings("javadoc")
public class ExpressionParser {
  public static final String LT = "$lt";
  public static final String EQ = "$eq";
  public static final String LTE = "$lte";
  public static final String GT = "$gt";
  public static final String GTE = "$gte";
  public static final String NE = "$ne";
  public static final String ALL = "$all";
  public static final String EXISTS = "$exists";
  public static final String MOD = "$mod";
  public static final String IN = "$in";
  public static final String NIN = "$nin";
  public static final String SIZE = "$size";
  public static final String NOT = "$not";
  public static final String OR = "$or";
  public static final String NOR = QueryOperators.NOR;
  public static final String AND = "$and";
  public static final String REGEX = "$regex";
  public static final String REGEX_OPTIONS = "$options";
  public static final String TYPE = "$type";
  public static final String NEAR = QueryOperators.NEAR;
  public static final String NEAR_SPHERE = QueryOperators.NEAR_SPHERE;
  public static final String MAX_DISTANCE = "$maxDistance";
  public static final String ELEM_MATCH = QueryOperators.ELEM_MATCH;
  public static final String WHERE = QueryOperators.WHERE;
  public static final String GEO_WITHIN = "$geoWithin";
  public static final String GEO_INTERSECTS = "$geoIntersects";
  public static final String SLICE = "$slice";
  public static final Filter AllFilter = new Filter() {
    @Override
    public boolean apply(DBObject o) {
      return true;
    }
  };

  // TODO : http://docs.mongodb.org/manual/reference/operator/query-geospatial/
  // TODO : http://docs.mongodb.org/manual/reference/operator/geoWithin/#op._S_geoWithin
  // TODO : http://docs.mongodb.org/manual/reference/operator/geoIntersects/
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionParser.class);
  private static final Map<Class, Integer> CLASS_TO_WEIGHT;

  static {
    // Sort order per http://docs.mongodb.org/manual/reference/operator/aggregation/sort/
    Map<Class, Integer> map = new HashMap<Class, Integer>();
    map.put(MinKey.class, Integer.MIN_VALUE);
    map.put(Null.class, 0);
    map.put(Double.class, 1);
    map.put(Float.class, 1);
    map.put(Integer.class, 1);
    map.put(Long.class, 1);
    map.put(Short.class, 1);
    map.put(String.class, 2);
    map.put(Object.class, 3);
    map.put(BasicDBObject.class, 4);
    map.put(LazyDBObject.class, 4);
    map.put(BasicDBList.class, 5);
    map.put(LazyBSONList.class, 5);
    map.put(byte[].class, 6);
    map.put(Binary.class, 6);
    map.put(ObjectId.class, 7);
    map.put(Boolean.class, 8);
    map.put(Date.class, 9);
    map.put(Pattern.class, 10);
    map.put(MaxKey.class, Integer.MAX_VALUE);

    CLASS_TO_WEIGHT = Collections.unmodifiableMap(map);
  }

  @SuppressWarnings("all")
  List<FilterFactory> filterFactories = Arrays.<FilterFactory>asList(
      new ConditionalOperatorFilterFactory(GTE) {
        @Override
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue, true);
          return result != null && result.intValue() <= 0;
        }
      },
      new ConditionalOperatorFilterFactory(LTE) {
        @Override
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue, true);
          return result != null && result.intValue() >= 0;
        }
      },
      new ConditionalOperatorFilterFactory(GT) {
        @Override
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue, true);
          return result != null && result.intValue() < 0;
        }
      },
      new ConditionalOperatorFilterFactory(LT) {
        @Override
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue, true);
          return result != null && result.intValue() > 0;
        }
      },
      new ConditionalOperatorFilterFactory(EQ) {
        @Override
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue, true);
          return result != null && result.intValue() == 0;
        }
      },
      new BasicCommandFilterFactory(NE) {
        @Override
        public Filter createFilter(final List<String> path, final DBObject refExpression) {
          return new Filter() {
            @Override
            public boolean apply(DBObject o) {
              Object queryValue = refExpression.get(command);
              List<Object> storedList = getEmbeddedValues(path, o);
              if (storedList.isEmpty()) {
                return queryValue != null;
              } else {
                for (Object storedValue : storedList) {
                  if (storedValue instanceof List) {
                    for (Object aValue : (List) storedValue) {
                      if (isEqual(queryValue, aValue)) {
                        return false;
                      }
                    }
                  }
                  if (isEqual(queryValue, storedValue)) {
                    return false;
                  }
                }
                return true;
              }
            }

            private boolean isEqual(Object obj1, Object obj2) {
              if (obj1 == null) {
                if (obj2 == null) {
                  return true;
                }

                return false;
              }

              Integer result = compareObjects(obj1, obj2, true);
              return result != null && result.intValue() == 0;
            }
          };
        }
      },
      new BasicFilterFactory(ALL) {
        @Override
        boolean compare(Object queryValue, Object storedValue) {
          Collection<?> queryList = typecast(command + " clause", queryValue, Collection.class);
          List storedList = typecast("value", storedValue, List.class);
          if (storedList == null) {
            return false;
          }

          if (queryList.isEmpty()) {
            return false;
          }

          for (Object queryObject : queryList) {
            if (queryObject instanceof Pattern) {
              if (!listContainsPattern(storedList, (Pattern) queryObject)) {
                return false;
              }
            } else if (queryObject instanceof DBObject
                && ((DBObject) queryObject).keySet().size() > 0
                && ((DBObject) queryObject).keySet().iterator().next().equals(ELEM_MATCH)) {
              // Run $elementMatch query on the list instead
              DBObject elemMatchQuery = new BasicDBObject("$$$$fongo$$$$", queryObject);
              Filter filter = buildFilter(elemMatchQuery);
              if (!filter.apply(new BasicDBObject("$$$$fongo$$$$", storedList))) {
                return false;
              }
            } else {
              if (!storedList.contains(queryObject)) {
                return false;
              }
            }
          }

          return true;
        }
      },
      new BasicFilterFactory(ELEM_MATCH) {
        @Override
        boolean compare(Object queryValue, Object storedValue) {
          DBObject query = castToDBObject(command + " clause", queryValue);
          List storedList = typecast("value", storedValue, List.class);
          if (storedList == null) {
            return false;
          }

          if (query.keySet().iterator().next().startsWith("$")) {
            // Simple expression, like $elemMatch: { $gte: 80, $lt: 85 }
            // We must iterate on each element and test with the query.
            BasicDBObject dbObject = new BasicDBObject("$$$$fongo$$$$", query);
            Filter filter = buildFilter(dbObject);
            for (Object object : storedList) {
              if (isDbObject(object)) {
                if (buildFilter(query).apply(toDbObject(object))) {
                  return true;
                }
              }
              if (filter.apply(new BasicDBObject("$$$$fongo$$$$", object))) {
                return true;
              }
            }
          } else {
            Filter filter = buildFilter(query);
            for (Object object : storedList) {
              if (filter.apply(toDbObject(object))) {
                return true;
              }
            }
          }

          return false;
        }
      },
      new BasicCommandFilterFactory(EXISTS) {
        @Override
        public Filter createFilter(final List<String> path, final DBObject refExpression) {
          return new Filter() {
            @Override
            public boolean apply(DBObject o) {
              List<Object> storedOption = getEmbeddedValues(path, o);
              return typecast(command + " clause", refExpression.get(command), Boolean.class) == !storedOption.isEmpty();
            }
          };
        }
      },
      new BasicFilterFactory(MOD) {

        @Override
        boolean compare(Object queryValue, Object storedValue) {
          List<Integer> queryList = typecast(command + " clause", queryValue, List.class);
          enforce(queryList.size() == 2, command + " clause must be a List of size 2");
          Number modulus = queryList.get(0);
          Number expectedValue = queryList.get(1);
          return (storedValue != null) && (typecast("value", storedValue, Number.class).longValue()) % modulus.longValue() == expectedValue.longValue();
        }
      },
      new InFilterFactory(IN, true),
      new InFilterFactory(NIN, false),
      new BasicFilterFactory(SIZE) {
        @Override
        boolean compare(Object queryValue, Object storedValue) {
          Number size = typecast(command + " clause", queryValue, Number.class);
          List storedList = typecast("value", storedValue, List.class);
          return storedList != null && storedList.size() == size.intValue();
        }
      },
      new BasicCommandFilterFactory(REGEX) {
        @Override
        public Filter createFilter(final List<String> path, DBObject refExpression) {
          String flagStr = typecast(REGEX_OPTIONS, refExpression.get(REGEX_OPTIONS), String.class);
          int flags = parseRegexOptionsToPatternFlags(flagStr);
          final Pattern pattern = Pattern.compile(refExpression.get(this.command).toString(), flags);

          return createPatternFilter(path, pattern);
        }
      },
      new NearCommandFilterFactory(NEAR_SPHERE, true),
      new NearCommandFilterFactory(NEAR, false),
      new GeoWithinCommandFilterFactory(GEO_WITHIN),
      new GeoIntersectsCommandFilterFactory(GEO_INTERSECTS),
      new BasicCommandFilterFactory(TYPE) {
        @Override
        public Filter createFilter(final List<String> path, DBObject refExpression) {
          Number type = typecast(TYPE, refExpression.get(TYPE), Number.class);

          return createTypeFilter(path, type.intValue());
        }
      }
  );

  public ObjectComparator objectComparator(int sortDirection) {
    if (!(sortDirection == -1 || sortDirection == 1)) {
      throw new FongoException("The $sort element value must be either 1 or -1. Actual: " + sortDirection);
    }
    return new ObjectComparator(sortDirection == 1);
  }

  public SortSpecificationComparator sortSpecificationComparator(DBObject orderBy) {
    return new SortSpecificationComparator(orderBy);
  }

  private boolean isDBObjectButNotDBList(Object o) {
    return isDbObject(o) && !(o instanceof List);
  }

  public Filter buildFilter(DBObject ref) {
    AndFilter andFilter = new AndFilter();
    if (ref != null) {
      for (String key : ref.keySet()) {
        Object expression = ref.get(key);

        andFilter.addFilter(buildExpressionFilter(key, expression));
      }
    }
    return andFilter;
  }

  public ValueFilter buildValueFilter(DBObject ref) {
    if (ref.containsField("$in")) {
      // Special case: $in inside $pull may filter primitive values, not DBObjects
      Collection<?> options = (Collection<?>) ref.get("$in");
      return buildValueFilterIn(options);
    }
    return buildValueFilter(buildFilter(ref));
  }

  private ValueFilter buildValueFilterIn(Collection<?> options) {
    final List<ValueFilter> filters = new ArrayList<ValueFilter>(options.size());
    for (Object option : options) {
      filters.add(buildValueFilter(option));
    }
    return new ValueFilter() {
      @Override
      public boolean apply(Object object) {
        for (ValueFilter filter : filters) {
          if (filter.apply(object)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private ValueFilter buildValueFilter(final Filter filter) {
    return new ValueFilter() {
      @Override
      public boolean apply(Object object) {
        return isDbObject(object) && filter.apply(toDbObject(object));
      }
    };
  }

  private ValueFilter buildValueFilter(final Object object) {
    if (isDbObject(object)) {
      return buildValueFilter(buildFilter(toDbObject(object)));
    }
    if (object instanceof Pattern) {
      return buildValueFilter((Pattern) object);
    }
    return new ValueFilter() {
      @Override
      public boolean apply(Object matchWith) {
        return Integer.valueOf(0).equals(compareObjects(object, matchWith, false));
      }
    };
  }

  private ValueFilter buildValueFilter(final Pattern pattern) {
    return new ValueFilter() {
      @Override
      public boolean apply(Object object) {
        return object instanceof String && pattern.matcher(object.toString()).find();
      }
    };
  }

  /**
   * Only build the filter for this keys.
   *
   * @param ref  query for filter.
   * @param keys must match to build the filter.
   */
  public Filter buildFilter(DBObject ref, Collection<String> keys) {
    AndFilter andFilter = new AndFilter();
    for (String key : ref.keySet()) {
      if (keys.contains(key)) {
        Object expression = ref.get(key);
        andFilter.addFilter(buildExpressionFilter(key, expression));
      }
    }
    return andFilter;
  }

  <T> T typecast(String fieldName, Object obj, Class<T> clazz) {
    try {
      return clazz.cast(obj);
    } catch (Exception e) {
      throw new FongoException(fieldName + " expected to be of type " + clazz.getName() + " but is " + (obj != null ? obj.getClass() : "null") + " toString:" + obj);
    }
  }

  private DBObject castToDBObject(String fieldName, Object obj) {
    if (isDbObject(obj)) {
      return toDbObject(obj);
    }
    throw new FongoException(fieldName + " expected to be DBObject or Map but is " + (obj != null ? obj.getClass() : "null") + " toString:" + obj);
  }

  private void enforce(boolean check, String message) {
    if (!check) {
      throw new FongoException(message);
    }
  }

  boolean objectMatchesPattern(Object obj, Pattern pattern) {
    if (obj instanceof CharSequence) {
      if (pattern.matcher((CharSequence) obj).find()) {
        return true;
      }
    }
    return false;
  }

  boolean listContainsPattern(List<Object> list, Pattern pattern) {
    for (Object obj : list) {
      if (objectMatchesPattern(obj, pattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * http://docs.mongodb.org/manual/reference/operator/type
   * <p/>
   * Type	Number
   * Double	1
   * String	2
   * Object	3
   * Array	4
   * Binary data	5
   * Undefined (deprecated)	6
   * Object id	7
   * Boolean	8
   * Date	9
   * Null	10
   * Regular Expression	11
   * JavaScript	13
   * Symbol	14
   * JavaScript (with scope)	15
   * 32-bit integer	16
   * Timestamp	17
   * 64-bit integer	18
   * Min key	255
   * Max key	127
   */
  boolean objectMatchesType(Object obj, int type) {
    switch (type) {
      case 1:
        return obj instanceof Double || obj instanceof Float;
      case 2:
        return obj instanceof CharSequence;
      case 3:
        return isDBObjectButNotDBList(obj);
      case 4:
        return obj instanceof List;
      case 7:
        return obj instanceof ObjectId;
      case 8:
        return obj instanceof Boolean;
      case 9:
        return obj instanceof Date;
      case 10:
        return obj == null;
      case 11:
        return obj instanceof Pattern;
      case 16:
        return obj instanceof Integer;
      case 18:
        return obj instanceof Long;
    }
    return false;
  }

  public List<Object> getEmbeddedValues(List<String> path, DBObject dbo) {
    return getEmbeddedValues(path, 0, dbo);
  }

  public List<Object> getEmbeddedValues(String key, DBObject dbo) {
    return getEmbeddedValues(Util.split(key), 0, dbo);
  }

  public List<Object> extractDBRefValue(DBRef ref, String refKey) {
    if ("$id".equals(refKey)) {
      return Collections.singletonList(ref.getId());
    } else if ("$ref".equals(refKey)) {
      return Collections.<Object>singletonList(ref.getCollectionName());
    } else return Collections.emptyList();
  }

  public List<Object> getEmbeddedValues(List<String> path, int startIndex, DBObject dbo) {
    String subKey = path.get(startIndex);
    if (path.size() > 1 && LOG.isDebugEnabled()) {
      LOG.debug("getEmbeddedValue looking for {} in {}", path, dbo);
    }

    for (int i = startIndex; i < path.size() - 1; i++) {
      Object value = dbo.get(subKey);
      if (isDbObject(value) && !(value instanceof List)) {
        dbo = toDbObject(value);
      } else if (value instanceof List && Util.isPositiveInt(path.get(i + 1))) {
        BasicDBList newList = Util.wrap((List) value);
        dbo = newList;
      } else if (value instanceof List) {
        List<Object> results = new ArrayList<Object>();
        for (Object listValue : (List) value) {
          if (isDbObject(listValue)) {
            List<Object> embeddedListValue = getEmbeddedValues(path, i + 1, toDbObject(listValue));
            results.addAll(embeddedListValue);
          } else if (listValue instanceof DBRef) {
            results.addAll(extractDBRefValue((DBRef) listValue, path.get(i + 1)));
          }
        }
        return results;
      } else if (value instanceof DBRef) {
        return extractDBRefValue((DBRef) value, path.get(i + 1));
      } else {
        return Collections.emptyList();
      }
      subKey = path.get(i + 1);
    }
    if (dbo.containsField(subKey)) {
      return Collections.singletonList((dbo.get(subKey)));
    } else {
      return Collections.emptyList();
    }
  }

  private Filter buildExpressionFilter(final String key, final Object expression) {
    return buildExpressionFilter(Util.split(key), expression);
  }

  private Filter buildExpressionFilter(final List<String> path, Object expression) {
    if (OR.equals(path.get(0))) {
      Collection<?> queryList = typecast(path + " operator", expression, Collection.class);
      OrFilter orFilter = new OrFilter();
      if (queryList.isEmpty()) {
        throw new FongoException(2, "$and/$or/$nor must be a nonempty array");
      }

      for (Object query : queryList) {
        orFilter.addFilter(buildFilter(toDbObject(query)));
      }
      return orFilter;
    } else if (NOR.equals(path.get(0))) {
      @SuppressWarnings(
          "unchecked") Collection<DBObject> queryList = typecast(path + " operator", expression, Collection.class);
      OrFilter orFilter = new OrFilter();
      if (queryList.isEmpty()) {
        throw new FongoException(2, "$and/$or/$nor must be a nonempty array");
      }

      for (DBObject query : queryList) {
        orFilter.addFilter(buildFilter(query));
      }
      return new NotFilter(orFilter);
    } else if (AND.equals(path.get(0))) {
      Collection<?> queryList = typecast(path + " operator", expression, Collection.class);
      if (queryList.isEmpty()) {
        throw new FongoException(2, "$and/$or/$nor must be a nonempty array");
      }
      AndFilter andFilter = new AndFilter();
      for (Object query : queryList) {
        andFilter.addFilter(buildFilter(toDbObject(query)));
      }
      return andFilter;
    } else if (WHERE.equals(path.get(0))) {
      return new WhereFilter((String) expression);
    } else if (isDbObject(expression)) {
      DBObject ref = toDbObject(expression);

      if (ref.containsField(NOT)) {
        return new NotFilter(buildExpressionFilter(path, ref.get(NOT)));
      } else {

        AndFilter andFilter = new AndFilter();
        int matchCount = 0;
        for (FilterFactory filterFactory : filterFactories) {
          if (filterFactory.matchesCommand(ref)) {
            matchCount++;
            andFilter.addFilter(filterFactory.createFilter(path, ref));
          }
        }
        if (matchCount == 0) {
          return simpleFilter(path, expression);
        }
        // WDEL : remove when trying to correct #201
//        if (matchCount > 2) {
//          throw new FongoException("Invalid expression for key " + path + ": " + expression);
//        }
        return andFilter;
      }
    } else if (expression instanceof Pattern) {
      return createPatternFilter(path, (Pattern) expression);
    } else {
      return simpleFilter(path, expression);
    }
  }

  public static DBObject toDbObject(Object expression) {
    if (expression == null) {
      return null;
    }
    if (expression instanceof DBObject) {
      return (DBObject) expression;
    }
    if (expression instanceof Map) {
      return new BasicDBObject((Map) expression);
    }
    throw new IllegalArgumentException("Expected DBObject or Map, got: " + expression);
  }

  public static boolean isDbObject(Object expression) {
    return expression instanceof DBObject || expression instanceof Map;
  }

  public Filter simpleFilter(final List<String> path, final Object expression) {
    return new Filter() {
      @Override
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return (expression == null);
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue instanceof List) {
              if (expression instanceof Collection) {
                return compareLists((List) storedValue, Util.toList((Collection) expression)) == 0;
              } else {
                return contains((List) storedValue, expression);
              }
            } else {
              if (expression == null) {
                return (storedValue == null);
              }
              if (compareObjects(expression, storedValue) == 0L) {
                return true;
              }
            }
          }
          return false;
        }

      }
    };
  }

  /**
   * Compare objects between {@code queryValue} and {@code storedValue}.
   * Can return null if {@code comparableFilter} is true and {@code queryValue} and {@code storedValue} can't be compared.
   */
  int compareObjects(Object queryValue, Object storedValue) {
    return compareObjects(queryValue, storedValue, false).intValue();
  }

  /**
   * Compare objects between {@code queryValue} and {@code storedValue}.
   * Can return null if {@code comparableFilter} is true and {@code queryValue} and {@code storedValue} can't be compared.
   *
   * @param queryValue
   * @param storedValue
   * @param comparableFilter if true, return null if {@code queryValue} and {@code storedValue} can't be compared..
   * @return
   */
  @SuppressWarnings("all")
  private Integer compareObjects(Object queryValue, Object storedValue, boolean comparableFilter) {
    LOG.debug("comparing {} and {}", queryValue, storedValue);

    if (isDBObjectButNotDBList(queryValue) && isDBObjectButNotDBList(storedValue)) {
      return compareDBObjects(toDbObject(queryValue), toDbObject(storedValue));
    } else if (queryValue instanceof List && storedValue instanceof List) {
      List queryList = (List) queryValue;
      List storedList = (List) storedValue;
      return compareLists(queryList, storedList, comparableFilter);
    } else if (queryValue!=null && storedValue != null
        && queryValue.getClass().isArray() && storedValue.getClass().isArray()) {
      return compareArrays(queryValue, storedValue, comparableFilter);
    } else {
      Object queryComp = typecast("query value", queryValue, Object.class);
      if (comparableFilter && !(storedValue instanceof Comparable)) {
        if (queryComp == storedValue || (queryComp!= null && queryComp.equals(storedValue))) {
          return 0;
        }
        return null;
      }
      Object storedComp = typecast("stored value", storedValue, Object.class);
      return compareTo(queryComp, storedComp, comparableFilter);
    }
  }

  protected int compareTo(Object c1, Object c2) { // Object to handle MinKey/MaxKey
    return compareTo(c1, c2, false);
  }

  /**
   * @param comparableFilter if true, return null if {@code queryValue} and {@code storedValue} can't be compared..
   */
  //@VisibleForTesting
  protected Integer compareTo(Object c1, Object c2, boolean comparableFilter) { // Object to handle MinKey/MaxKey
    Object cc1 = c1;
    Object cc2 = c2;
    Class<?> clazz1 = c1 == null ? Null.class : c1.getClass();
    Class<?> clazz2 = c2 == null ? Null.class : c2.getClass();
    // Not comparable for MinKey/MaxKey
    if (!clazz1.isAssignableFrom(clazz2) || !(cc1 instanceof Comparable)) {
      boolean checkTypes = true;
      if (cc1 instanceof Number) {
        if (cc2 instanceof Number) {
          cc1 = new BigDecimal(cc1.toString());
          cc2 = new BigDecimal(cc2.toString());
          checkTypes = false;
        }
      }
      if (cc1 instanceof Binary) {
        cc1 = convertFrom((Binary) cc1);
        checkTypes = false;
      }
      if (cc2 instanceof Binary) {
        cc2 = convertFrom((Binary) cc2);
        checkTypes = false;
      }
      if (cc1 instanceof byte[]) {
        cc1 = convertFrom((byte[]) cc1);
        checkTypes = false;
      }
      if (cc2 instanceof byte[]) {
        cc2 = convertFrom((byte[]) cc2);
        checkTypes = false;
      }
//      if (cc1 instanceof ObjectId && cc2 instanceof String && ObjectId.isValid((String) cc2)) {
//        cc2 = ObjectId.massageToObjectId(cc2);
//        checkTypes = false;
//      }
//      if (cc2 instanceof ObjectId && cc1 instanceof String && ObjectId.isValid((String) cc1)) {
//        cc1 = ObjectId.massageToObjectId(cc2);
//        checkTypes = false;
//      }
      Coordinate ll1 = GeoUtil.coordinate(cc1);
      if (ll1 != null) {
        Coordinate ll2 = GeoUtil.coordinate(cc2);
        if (ll2 != null) {
          cc1 = ll1;
          cc2 = ll2;
          checkTypes = false;
        }
      }
      if (cc1 instanceof DBRef && cc2 instanceof DBRef) {
        DBRef a1 = (DBRef) cc1;
        DBRef a2 = (DBRef) cc2;
        if (a1.equals(a2)) {
          return 0;
        }
        // Not the idea of the year..
        cc1 = a1.toString();
        cc2 = a2.toString();
        checkTypes = false;
      }
      if (cc1 instanceof UUID && !(cc2 instanceof UUID)) {
        return -1;
      }
      if (cc2 instanceof UUID && !(cc1 instanceof UUID)) {
        return 1;
      }
      if (checkTypes) {
        Integer type1 = CLASS_TO_WEIGHT.get(clazz1);
        Integer type2 = CLASS_TO_WEIGHT.get(clazz2);
        if (type1 != null && type2 != null) {
          cc1 = type1;
          cc2 = type2;
        } else {
          if (!comparableFilter) {
            throw new FongoException("Don't know how to compare " + cc1.getClass() + " and " + cc2.getClass() + " values are : " + c1 + " vs " + c2);
          } else {
            return null;
          }
        }
      }
    }

//    LOG.info("\tcompareTo() cc1:[{}], cc2:[{}] => {}", cc1, cc2, ((Comparable) cc1).compareTo(cc2));
    return ((Comparable) cc1).compareTo(cc2);
  }

  private Comparable<String> convertFrom(Binary binary) {
    return new String(binary.getData()); // + binary.getType(); // Adding getType() to respect contract of "equals";
  }

  private Comparable<String> convertFrom(byte[] array) {
    return new String(array);
  }

  private boolean contains(Collection source, Object element) {
    for (Object objectSource : source) {
      if (compareObjects(objectSource, element) == 0) {
        return true;
      }
    }
    return false;
  }

  private boolean contains(Collection source, Collection elements) {
    for (Object element : elements) {
      if (!contains(source, element)) {
        return false;
      }
    }
    return true;
  }


  public int compareLists(List queryList, List storedList) {
    return compareLists(queryList, storedList, false);
  }

  /**
   * @param comparableFilter if true, return null if {@code queryValue} and {@code storedValue} can't be compared..
   */
  private Integer compareLists(List queryList, List storedList, boolean comparableFilter) {
    int sizeDiff = queryList.size() - storedList.size();
    if (sizeDiff != 0) {
      if (sizeDiff > 0 && queryList.get(storedList.size()) instanceof MinKey) {
        return -1; // Minkey is ALWAYS first, even if other is null
      }
      if (sizeDiff < 0 && storedList.get(queryList.size()) instanceof MinKey) {
        return 1; // Minkey is ALWAYS first, even if other is null
      }
      if (sizeDiff < 0 && storedList.get(queryList.size()) instanceof MaxKey) {
        return -1; // MaxKey is ALWAYS last, even if other is null
      }
      if (sizeDiff > 0 && queryList.get(storedList.size()) instanceof MaxKey) {
        return 1; // MaxKey is ALWAYS last, even if other is null
      }
      // Special case : {x : null} and "no x" is equal.
      boolean bothEmpty = isEmptyOrContainsOnlyNull(storedList) && isEmptyOrContainsOnlyNull(queryList);
      if (bothEmpty) {
        return 0;
      }
      return sizeDiff;
    }
    for (int i = 0, length = queryList.size(); i < length; i++) {
      Integer compareValue = compareObjects(queryList.get(i), storedList.get(i), comparableFilter);
      if (compareValue == null) {
        return -1; // Arbitrary
      }
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0;
  }

  private Integer compareArrays(Object queryList, Object storedList, boolean comparableFilter) {
    int queryListSize = Array.getLength(queryList);
    int storedListSize = Array.getLength(storedList);

    int sizeDiff = Array.getLength(queryList) - storedListSize;
    if (sizeDiff != 0) {
      if (sizeDiff > 0 && Array.get(queryList, storedListSize) instanceof MinKey) {
        return -1; // Minkey is ALWAYS first, even if other is null
      }
      if (sizeDiff < 0 && Array.get(storedList, queryListSize) instanceof MinKey) {
        return 1; // Minkey is ALWAYS first, even if other is null
      }
      if (sizeDiff < 0 && Array.get(storedList, queryListSize) instanceof MaxKey) {
        return -1; // MaxKey is ALWAYS last, even if other is null
      }
      if (sizeDiff > 0 && Array.get(queryList, storedListSize) instanceof MaxKey) {
        return 1; // MaxKey is ALWAYS last, even if other is null
      }
      // Special case : {x : null} and "no x" is equal.
      boolean bothEmpty = isArrayEmptyOrContainsOnlyNull(storedList) && isArrayEmptyOrContainsOnlyNull(queryList);
      if (bothEmpty) {
        return 0;
      }
      return sizeDiff;
    }

    for (int i = 0, length = queryListSize; i < length; i++) {
      Integer compareValue = compareObjects(Array.get(queryList, i), Array.get(storedList, i), comparableFilter);
      if (compareValue == null) {
        return -1; // Arbitrary
      }
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0;
  }

  private boolean isEmptyOrContainsOnlyNull(List list) {
    for (Object obj : list) {
      if (obj != null) {
        return false;
      }
    }
    return true;
  }

  private boolean isArrayEmptyOrContainsOnlyNull(Object array) {
    int arraySize = Array.getLength(array);

    for (int i=0; i<arraySize; i++) {
      Object obj = Array.get(array, i);
      if (obj != null) {
        return false;
      }
    }
    return true;
  }

  private int compareDBObjects(DBObject db0, DBObject db1) {
    Iterator<String> i0 = db0.keySet().iterator();
    Iterator<String> i1 = db1.keySet().iterator();

    while (i0.hasNext() || i1.hasNext()) {
      String key0 = i0.hasNext() ? i0.next() : null;
      String key1 = i1.hasNext() ? i1.next() : null;

      int keyComparison = Util.compareToNullable(key0, key1);
      if (keyComparison != 0) {
        return keyComparison;
      }

      Object value0 = key0 == null ? null : db0.get(key0);
      Object value1 = key1 == null ? null : db1.get(key1);

      int valueComparison = compareObjects(value0, value1);
      if (valueComparison != 0) {
        return valueComparison;
      }
    }

    return 0;
  }

  public Filter createPatternFilter(final List<String> path, final Pattern pattern) {
    return new Filter() {
      @Override
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return false;
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue != null) {
              if (storedValue instanceof List) {
                if (listContainsPattern((List) storedValue, pattern)) {
                  return true;
                }
              } else if (objectMatchesPattern(storedValue, pattern)) {
                return true;
              }
            }
          }
          return false;
        }
      }
    };
  }

  public Filter createTypeFilter(final List<String> path, final int type) {
    return new Filter() {
      @Override
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return false;
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue instanceof Collection) {
              for (Object object : (Collection) storedValue) {
                if (objectMatchesType(object, type)) {
                  return true;
                }
              }
            } else if (objectMatchesType(storedValue, type)) {
              return true;
            }
          }
          return false;
        }
      }
    };
  }

  // Take care of : https://groups.google.com/forum/?fromgroups=#!topic/mongomapper/MfRDh2vtCFg
  public Filter createNearFilter(final List<String> path, final Number maxDistance, final Geometry geometry, final boolean sphere) {
    return new Filter() {
      @Override
      public boolean apply(DBObject o) {
        final Geometry objectGeometry = GeoUtil.toGeometry(toDbObject(Util.extractField(o, path)));

        double distance = GeoUtil.distanceInRadians(geometry, objectGeometry, sphere);
        o.put(FongoDBCollection.FONGO_SPECIAL_ORDER_BY, distance);
        return maxDistance == null || distance < maxDistance.doubleValue();
      }
    };
  }

  private Filter createGeowithinFilter(final List<String> path, final Geometry geometry) {
    return new Filter() {

      @Override
      public boolean apply(DBObject o) {

        Geometry local = GeoUtil.toGeometry(java.util.Optional.ofNullable(Util.extractField(o, path)));
        return GeoUtil.geowithin(local, geometry);
      }
    };
  }

  private Filter createGeointersectsFilter(final List<String> path, final Geometry geometry) {
    return new Filter() {

      @Override
      public boolean apply(DBObject o) {

        Geometry local = GeoUtil.toGeometry(java.util.Optional.ofNullable(Util.extractField(o, path)));
        return GeoUtil.geowithin(local, geometry);
      }
    };
  }

  public int parseRegexOptionsToPatternFlags(String flagString) {
    int flags = 0;
    for (int i = 0; flagString != null && i < flagString.length(); i++) {
      switch (flagString.charAt(i)) {
        case 'i':
          flags |= Pattern.CASE_INSENSITIVE;
          break;
        case 'x':
          flags |= Pattern.COMMENTS;
          break;
        case 'm':
          flags |= Pattern.MULTILINE;
          break;
        case 's':
          flags |= Pattern.DOTALL;
          break;
      }
    }
    return flags;
  }

  public ObjectComparator buildObjectComparator(boolean asc) {
    return new ObjectComparator(asc);
  }

  interface FilterFactory {
    public boolean matchesCommand(DBObject refExpression);

    public Filter createFilter(List<String> path, DBObject refExpression);
  }

  private static class Null {
  }

  static class NotFilter implements Filter {
    private final Filter filter;

    public NotFilter(Filter filter) {
      this.filter = filter;
    }

    @Override
    public boolean apply(DBObject o) {
      return !filter.apply(o);
    }

  }

  static abstract class ConjunctionFilter implements Filter {

    final List<Filter> filters = new ArrayList<Filter>();

    public void addFilter(Filter filter) {
      filters.add(filter);
    }

  }

  static class AndFilter extends ConjunctionFilter {
    @Override
    public boolean apply(DBObject o) {
      for (Filter f : filters) {
        if (!f.apply(o)) {
          return false;
        }
      }
      return true;
    }
  }

  static class OrFilter extends ConjunctionFilter {
    @Override
    public boolean apply(DBObject o) {
      for (Filter f : filters) {
        if (f.apply(o)) {
          return true;
        }
      }
      return false;
    }

  }

  public class ObjectComparator implements Comparator {
    private final int asc;

    ObjectComparator(boolean asc) {
      this.asc = asc ? 1 : -1;
    }

    @Override
    public int compare(Object o1, Object o2) {
      return asc * compareObjects(o1, o2);
    }
  }

  public class SortSpecificationComparator implements Comparator<Object> {

    private final DBObject orderBy;
    private final Set<String> orderByKeySet;

    public SortSpecificationComparator(DBObject orderBy) {
      this.orderBy = orderBy;
      this.orderByKeySet = orderBy.keySet();

      if (this.orderByKeySet.isEmpty()) {
        throw new FongoException("The $sort pattern is empty when it should be a set of fields.");
      }
    }

    @Override
    public int compare(Object o1, Object o2) {
      if (isDBObjectButNotDBList(o1) && isDBObjectButNotDBList(o2)) {
        DBObject dbo1 = toDbObject(o1);
        DBObject dbo2 = toDbObject(o2);
        for (String sortKey : orderByKeySet) {
          final List<String> path = Util.split(sortKey);
          int sortDirection = (Integer) orderBy.get(sortKey);

          List<Object> o1list = getEmbeddedValues(path, dbo1);
          List<Object> o2list = getEmbeddedValues(path, dbo2);

          int compareValue = compareLists(o1list, o2list, false) * sortDirection;
          if (compareValue != 0) {
            return compareValue;
          }
        }
        return 0;
      } else if (isDBObjectButNotDBList(o1) || isDBObjectButNotDBList(o2)) {
        DBObject dbo = toDbObject(isDbObject(o1) ? o1 : o2);
        for (String sortKey : orderByKeySet) {
          final List<String> path = Util.split(sortKey);
          int sortDirection = (Integer) orderBy.get(sortKey);

          List<Object> foundValues = getEmbeddedValues(path, dbo);

          if (!foundValues.isEmpty()) {
            return isDbObject(o1) ? sortDirection : -sortDirection;
          }
        }
        return compareTo(o1, o2);
      } else {
        return compareTo(o1, o2);
      }
    }
  }

  abstract class BasicCommandFilterFactory implements FilterFactory {

    public final String command;

    public BasicCommandFilterFactory(final String command) {
      this.command = command;
    }

    @Override
    public boolean matchesCommand(DBObject refExpression) {
      return refExpression.containsField(command);
    }
  }

  abstract class BasicFilterFactory extends BasicCommandFilterFactory {
    public BasicFilterFactory(final String command) {
      super(command);
    }

    @Override
    public boolean matchesCommand(DBObject refExpression) {
      return refExpression.containsField(command);
    }

    @Override
    public Filter createFilter(final List<String> path, final DBObject refExpression) {
      return new Filter() {
        @Override
        public boolean apply(DBObject o) {
          List<Object> storedList = getEmbeddedValues(path, o);
          if (storedList.isEmpty()) {
            return false;
          } else {
            for (Object storedValue : storedList) {
              if (compare(refExpression.get(command), storedValue)) {
                return true;
              }
            }
            return false;
          }
        }
      };
    }

    abstract boolean compare(Object queryValue, Object storedValue);

  }

  private final class WhereFilter implements Filter {
    private final String expression;

    public WhereFilter(String expression) {
      this.expression = expression;
    }

    @Override
    public boolean apply(DBObject o) {
      Context cx = Context.enter();

      try {
        Scriptable scope = cx.initStandardObjects();
        String json = FongoJSON.serialize(o);
        String expr = "obj=" + json + ";\n" + expression.replace("this.", "obj.") + ";\n";
        try {
          return (Boolean) cx.evaluateString(scope, expr, "<$where>", 0, null);
        } catch (Exception e) {
          LOG.error("Exception evaluating javascript expression {}", expression, e);
        }
      } finally {
        cx.exit();
      }

      return false;
    }
  }

  @SuppressWarnings("all")
  private final class InFilterFactory extends BasicCommandFilterFactory {

    private final boolean direction;

    public InFilterFactory(String command, boolean direction) {
      super(command);
      this.direction = direction;
    }

    @Override
    public Filter createFilter(final List<String> path, final DBObject refExpression) {
      final Collection queryList;
      final Object expression = refExpression.get(command);
      if (expression.getClass().isArray()) {
        queryList = Util.toCollection(expression);
      } else {
        queryList = typecast(command + " clause", expression, Collection.class);
      }
      final Set<?> querySet = new HashSet<Object>(queryList);
      return new Filter() {
        @Override
        public boolean apply(DBObject o) {
          List<Object> storedList = getEmbeddedValues(path, o);
          if (storedList.isEmpty()) {
            // Special case: Querying for null should return positive if the field is absent.
            // See: http://docs.mongodb.org/manual/faq/developers/#faq-developers-query-for-nulls
            return querySet.contains(null) ? direction : !direction;
          } else {
            for (Object storedValue : storedList) {
              if (compare(expression, storedValue, querySet) == direction) {
                return direction;
              }
            }
            return !direction;
          }
        }
      };
    }

    boolean compare(Object queryValueIgnored, Object storedValue, Set querySet) {
      if (storedValue instanceof List) {
        for (Object valueItem : (List) storedValue) {
          if (containsWithRegex(querySet, valueItem)) {
            return direction;
          }
        }
        if (containsWithRegex(querySet, storedValue)) {
          return direction;
        }
        return !direction;
      } else {
        return !(direction ^ containsWithRegex(querySet, storedValue));
      }
    }

    boolean containsWithRegex(Set querySet, Object storedValue) {
      for (Object queryObject : querySet) {
        if (Integer.valueOf(0).equals(compareObjects(queryObject, storedValue, true))) {
          return true;
        }
      }
      if (storedValue instanceof CharSequence) {
        CharSequence s = (CharSequence) storedValue;
        for (Object o : querySet) {
          if (o instanceof Pattern) {
            Pattern p = (Pattern) o;
            if (p.matcher(s).find()) {
              return true;
            }
          }
        }
      }
      return false;
    }
  }

  private final class NearCommandFilterFactory extends BasicCommandFilterFactory {

    final boolean spherical;

    public NearCommandFilterFactory(final String command, boolean spherical) {
      super(command);
      this.spherical = spherical;
    }

    // http://docs.mongodb.org/manual/reference/operator/near/#op._S_near
    @Override
    public Filter createFilter(final List<String> path, DBObject refExpression) {
      LOG.debug("path:{}, refExp:{}", path, refExpression);
      Number maxDistance;
      final Geometry geometry;
      if (refExpression.get(command) instanceof BasicDBList) {
        final List<Coordinate> coordinates = GeoUtil.coordinate(Collections.singletonList(command), refExpression);// typecast(command, refExpression.get(command), List.class);
        geometry = GeoUtil.createGeometryPoint(coordinates.get(0));
        maxDistance = typecast(MAX_DISTANCE, refExpression.get(MAX_DISTANCE), Number.class);
      } else {
        DBObject dbObject = castToDBObject(command, refExpression.get(command));
        geometry = GeoUtil.toGeometry((toDbObject(Util.extractField(dbObject, "$geometry"))));
        maxDistance = typecast(MAX_DISTANCE, dbObject.get(MAX_DISTANCE), Number.class);
        if (maxDistance != null) {
          // When in GeoJSon, distance is in meter.
          maxDistance = maxDistance.doubleValue() / GeoUtil.EARTH_RADIUS;
        }
      }
      return createNearFilter(path, maxDistance, geometry, spherical);
    }
  }

  private final class GeoWithinCommandFilterFactory extends BasicCommandFilterFactory {

    public GeoWithinCommandFilterFactory(final String command) {
      super(command);
    }

    // http://docs.mongodb.org/manual/reference/operator/query/geoWithin/
    @Override
    public Filter createFilter(final List<String> path, DBObject refExpression) {
      LOG.debug(command + " path:{}, refExp:{}", path, refExpression);
      Geometry geometry = GeoUtil.toGeometry(castToDBObject(command, refExpression.get(command)));
      return createGeointersectsFilter(path, geometry);
    }
  }

  private final class GeoIntersectsCommandFilterFactory extends BasicCommandFilterFactory {

    public GeoIntersectsCommandFilterFactory(final String command) {
      super(command);
    }

    // https://docs.mongodb.com/manual/reference/operator/query/geoIntersects/
    @Override
    public Filter createFilter(final List<String> path, DBObject refExpression) {
      LOG.debug(command + " path:{}, refExp:{}", path, refExpression);
      DBObject geoIntersect = castToDBObject(command, refExpression.get(command));
      if (geoIntersect.get("$geometry") == null) {
        throw new FongoException(2, "Query failed with error code 2 and error message '$geoIntersect not supported with provided geometry: " + refExpression);
      }
      final DBObject dbObjectGeometry = castToDBObject("$geometry", geoIntersect.get("$geometry"));
      try {
        Geometry geometry = GeoUtil.toGeometry(dbObjectGeometry);
        return createGeowithinFilter(path, geometry);
      } catch (IllegalArgumentException iea) {
        throw new FongoException(2, "Query failed with error code 2 and error message 'Loop is not closed: " + dbObjectGeometry.get("coordinates"));
      }
    }
  }

  abstract class ConditionalOperatorFilterFactory extends BasicFilterFactory {

    public ConditionalOperatorFilterFactory(String command) {
      super(command);
    }

    @Override
    final boolean compare(Object queryValue, Object storedValue) {
      if (storedValue instanceof List) {
        for (Object aValue : (List) storedValue) {
          if (aValue != null && singleCompare(queryValue, aValue)) {
            return true;
          }
        }
        if (queryValue instanceof List) {
          List q = (List) queryValue;
          return q.isEmpty() && ((List) storedValue).isEmpty();
        }
        return false;
      } else {
        return storedValue != null && singleCompare(queryValue, storedValue);
      }
    }

    abstract boolean singleCompare(Object queryValue, Object storedValue);
  }
}
