package com.github.fakemongo.impl.aggregation;

import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Util;
import com.mongodb.*;
import com.mongodb.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * TODO : { project : { _id : 0} } must remove the _id field. If a $sort exist after...
 */
@ThreadSafe
public class Project extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Project.class);

  public static final Project INSTANCE = new Project();

  private Project() {
  }

  static abstract class ProjectedAbstract<T extends ProjectedAbstract> {
    static final Map<String, Class<? extends ProjectedAbstract>> projectedAbstractMap = new HashMap<String, Class<? extends ProjectedAbstract>>();

    static {
      projectedAbstractMap.put(ProjectedSize.KEYWORD, ProjectedSize.class);
      projectedAbstractMap.put(ProjectedStrcasecmp.KEYWORD, ProjectedStrcasecmp.class);
      projectedAbstractMap.put(ProjectedCmp.KEYWORD, ProjectedCmp.class);
      projectedAbstractMap.put(ProjectedCond.KEYWORD, ProjectedCond.class);
      projectedAbstractMap.put(ProjectedSubstr.KEYWORD, ProjectedSubstr.class);
      projectedAbstractMap.put(ProjectedIfNull.KEYWORD, ProjectedIfNull.class);
      projectedAbstractMap.put(ProjectedConcat.KEYWORD, ProjectedConcat.class);
      projectedAbstractMap.put(ProjectedToLower.KEYWORD, ProjectedToLower.class);
      projectedAbstractMap.put(ProjectedToUpper.KEYWORD, ProjectedToUpper.class);
      projectedAbstractMap.put(ProjectedToDivide.KEYWORD, ProjectedToDivide.class);
      projectedAbstractMap.put(ProjectedToMod.KEYWORD, ProjectedToMod.class);
      projectedAbstractMap.put(ProjectedToMultiply.KEYWORD, ProjectedToMultiply.class);
      projectedAbstractMap.put(ProjectedToAdd.KEYWORD, ProjectedToAdd.class);
      projectedAbstractMap.put(ProjectedToSubtract.KEYWORD, ProjectedToSubtract.class);
      projectedAbstractMap.put(ProjectedDateDayOfYear.KEYWORD, ProjectedDateDayOfYear.class);
      projectedAbstractMap.put(ProjectedDateDayOfMonth.KEYWORD, ProjectedDateDayOfMonth.class);
      projectedAbstractMap.put(ProjectedDateDayOfWeek.KEYWORD, ProjectedDateDayOfWeek.class);
      projectedAbstractMap.put(ProjectedDateYear.KEYWORD, ProjectedDateYear.class);
      projectedAbstractMap.put(ProjectedDateMonth.KEYWORD, ProjectedDateMonth.class);
      projectedAbstractMap.put(ProjectedDateWeek.KEYWORD, ProjectedDateWeek.class);
      projectedAbstractMap.put(ProjectedDateHour.KEYWORD, ProjectedDateHour.class);
      projectedAbstractMap.put(ProjectedDateMinute.KEYWORD, ProjectedDateMinute.class);
      projectedAbstractMap.put(ProjectedDateSecond.KEYWORD, ProjectedDateSecond.class);
      projectedAbstractMap.put(ProjectedDateMillisecond.KEYWORD, ProjectedDateMillisecond.class);
      projectedAbstractMap.put(ProjectedOr.KEYWORD, ProjectedOr.class);
      projectedAbstractMap.put(ProjectedAnd.KEYWORD, ProjectedAnd.class);

      projectedAbstractMap.put(ProjectedFilter.KEYWORD, ProjectedFilter.class);

    }

    final String keyword;

    final String destName;

    private ProjectedAbstract(String keyword, String destName, DBObject object) {
      this.keyword = keyword;
      this.destName = destName;
    }

    /**
     * Transform the "object" into the "result" with this "value"
     *
     * @param result
     * @param object
     * @param key
     */
    public abstract void unapply(DBObject result, DBObject object, String key);

    abstract void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace);

    public final void apply(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, DBObject value, String namespace) {
      doWork(coll, projectResult, projectedFields, key, value.get(this.keyword), namespace);
    }

    /**
     * Create the mapping and the criteria for the collection.
     *
     * @param projectResult   find criteria.
     * @param projectedFields mapping from criteria to project structure.
     * @param key             keyword from a DBObject.
     * @param kvalue          value for k from a DBObject.
     * @param namespace       "" if empty, "fieldname." elsewhere.
     * @param projected       use for unapplying.
     */
    public static void createMapping(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object kvalue, String namespace, ProjectedAbstract projected) {
      // Simple case : nb : "$pop"
      if (kvalue instanceof String) {
        String value = kvalue.toString();
        if (value.startsWith("$")) {
          // Case { date: "$date"}

          // Extract filename from projection.
          String fieldName = kvalue.toString().substring(1);
          // Prepare for renaming.
          multimapPut(projectedFields, fieldName, projected);
          projectResult.removeField(key);

          // Handle complex case like $bar.foo with a little trick.
          if (fieldName.contains(".")) {
            projectResult.put(fieldName.substring(0, fieldName.indexOf('.')), 1);
          } else {
            projectResult.put(fieldName, 1);
          }
        } else {
          multimapPut(projectedFields, value, projected);
        }
      } else if (ExpressionParser.isDbObject(kvalue)) {
        DBObject value = ExpressionParser.toDbObject(kvalue);
        ProjectedAbstract projectedAbstract = getProjected(value, coll, key);
        if (projectedAbstract != null) {
          // case : {cmp : {$cmp:[$firstname, $lastname]}}
          projectResult.removeField(key);
          projectedAbstract.apply(coll, projectResult, projectedFields, key, value, namespace);
        } else {
          // case : {biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" }}
          projectResult.removeField(key);
          for (Map.Entry<String, Object> subentry : Util.entrySet(value)) {
            createMapping(coll, projectResult, projectedFields, subentry.getKey(), subentry.getValue(), namespace + key + ".", ProjectedRename.newInstance(namespace + key + "." + subentry.getKey(), coll, null));
          }
        }
      } else {
        // Case: {date : 1}
        multimapPut(projectedFields, key, projected);
      }
    }

    private static void multimapPut(Map<String, List<ProjectedAbstract>> projectedFields, String key, ProjectedAbstract projected) {
      if (projectedFields.containsKey(key)) {
        projectedFields.get(key).add(projected);
      } else {
        List<ProjectedAbstract> list = new ArrayList<ProjectedAbstract>();
        list.add(projected);
        projectedFields.put(key, list);
      }
    }

    /**
     * Search the projected field if any.
     *
     * @param value    the DbObject being worked.
     * @param coll     collection used.
     * @param destName destination name for the field.
     * @return null if it's not a keyword.
     */
    private static ProjectedAbstract getProjected(DBObject value, DBCollection coll, String destName) {
      for (Map.Entry<String, Class<? extends ProjectedAbstract>> entry : projectedAbstractMap.entrySet()) {
        if (value.containsField(entry.getKey())) {
          try {
            return entry.getValue().getConstructor(String.class, DBCollection.class, DBObject.class).newInstance(destName, coll, value);
          } catch (InstantiationException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof MongoException) {
              throw (MongoException) e.getTargetException();
            }
            throw new RuntimeException(e);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
          }
        }
      }

      return null;
    }

    static void errorResult(DBCollection coll, int code, String err) {
      ((FongoDB) coll.getDB()).notOkErrorResult(code, err).throwOnError();
    }

    /**
     * Extract a value from a field name or value.
     */
    static <T> T extractValue(DBObject object, Object fieldOrValue) {
      if (fieldOrValue instanceof String && fieldOrValue.toString().startsWith("$")) {
        return Util.extractField(object, fieldOrValue.toString().substring(1));
      }
      return (T) fieldOrValue;
    }
  }

  static class ProjectedRename extends ProjectedAbstract<ProjectedRename> {
    public static final String KEYWORD = "$___fongo$internal$";

    private ProjectedRename(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
    }

    public static ProjectedRename newInstance(String destName, DBCollection coll, DBObject object) {
      return new ProjectedRename(destName, coll, object);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      if (key != null) {
        Object value = Util.extractField(object, key);
        Util.putValue(result, destName, value);
      }
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
    }
  }


  static class ProjectedSize extends ProjectedAbstract<ProjectedSize> {
    public static final String KEYWORD = "$size";

    private final String field;

    private final DBCollection coll;

    public ProjectedSize(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedSize(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      this.coll = coll;
      Object value = object.get(keyword);
      field = parseOperand(value);
    }

    private String parseOperand(Object value) {
      if (!(value instanceof String) && (!(value instanceof List) || ((List) value).size() != 1)) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array of 1 operand");
      }

      Object parsedValue = (value instanceof List) ? ((List) value).get(0) : value;
      return (String) parsedValue;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, destName, destName, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      int size = 0;
      Object value = extractValue(object, field);
      if (value instanceof Collection) {
        size = ((Collection) value).size();
      } else {
        errorResult(coll, 17124, "the " + KEYWORD + " operator requires an list.");
      }
      result.put(destName, size);
    }
  }

  static class ProjectedIfNull extends ProjectedAbstract<ProjectedIfNull> {
    public static final String KEYWORD = "$ifNull";

    private final String field;
    private final Object valueIfNull;

    public ProjectedIfNull(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the $ifNull operator requires an array of 2 operands");
      }
      @SuppressWarnings("unchecked") List<String> values = (List<String>) value;
      this.field = values.get(0);
      this.valueIfNull = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field, field, namespace, this);
      createMapping(coll, projectResult, projectedFields, String.valueOf(valueIfNull), valueIfNull, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object value = extractValue(object, field);
      if (value == null) {
        value = extractValue(object, valueIfNull);
      }
      result.put(destName, value);
    }
  }

  static class ProjectedConcat extends ProjectedAbstract<ProjectedConcat> {
    public static final String KEYWORD = "$concat";

    private List<Object> toConcat = null;

    public ProjectedConcat(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() == 0) {
        errorResult(coll, 16020, "the $concat operator requires an array of operands");
      }
      //noinspection unchecked
      toConcat = (List<Object>) value;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      for (Object field : toConcat) {
        if (field instanceof String) {
          createMapping(coll, projectResult, projectedFields, (String) field, field, namespace, this);
        } else if (ExpressionParser.isDbObject(field)) {
          // $concat : [ { $ifnull : [ "$item", "item is null" ] } ]
        }
      }
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      StringBuilder sb = new StringBuilder();
      for (Object info : toConcat) {
        Object value = extractValue(object, info);
        if (value == null) {
          result.put(destName, null);
          return;
        } else {
          String str = value.toString();
          sb.append(str);
        }
      }
      result.put(destName, sb.toString());
    }
  }

  static class ProjectedSubstr extends ProjectedAbstract<ProjectedSubstr> {
    public static final String KEYWORD = "$substr";

    private final String field;
    private final int start, end;

    public ProjectedSubstr(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 3) {
        errorResult(coll, 16020, "the $substr operator requires an array of 3 operands");
      }
      @SuppressWarnings("unchecked") List<Object> values = (List<Object>) value;
      field = (String) values.get(0);
      start = ((Number) values.get(1)).intValue();
      end = ((Number) values.get(2)).intValue();
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, destName, destName, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object exracted = extractValue(object, field);
      String value = exracted == null ? null : String.valueOf(exracted);
      if (value == null) {
        value = "";
      } else {
        if (start >= value.length()) {
          value = "";
        } else {
          value = value.substring(start, Math.min(end, value.length()));
        }
      }

      result.put(destName, value);
    }
  }

  static class ProjectedCmp extends ProjectedAbstract<ProjectedCmp> {
    public static final String KEYWORD = "$cmp";

    private final String field1;
    private final String field2;

    public ProjectedCmp(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    public ProjectedCmp(String keyword, String destName, DBCollection coll, DBObject object) {
      super(keyword, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array of 2 operands");
      }
      List<String> values = (List<String>) value;
      field1 = values.get(0);
      field2 = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field1, field1, namespace, this);
      createMapping(coll, projectResult, projectedFields, field2, field2, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      String value = extractValue(object, field1).toString();
      String secondValue = extractValue(object, field2).toString();
      int strcmp = compare(value, secondValue);
      result.put(destName, strcmp < 0 ? -1 : strcmp > 1 ? 1 : 0);
    }

    int compare(String value1, String value2) {
      return value1.compareTo(value2);
    }
  }

  static class ProjectedCond extends ProjectedAbstract<ProjectedCond> {
    static final String KEYWORD = "$cond";

    private final DBObject cond;
    private final Object cThen;
    private final Object cElse;

    public ProjectedCond(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedCond(String keyword, String destName, DBCollection coll, DBObject object) {
      super(keyword, destName, object);
      Object value = object.get(keyword);
      if ((value instanceof List)) {
        if (((List) value).size() != 3) {
          errorResult(coll, 16020, "the " + keyword + " operator requires an array of 3 operands");
        }
        List<Object> values = (List<Object>) value;
        cond = (DBObject) values.get(0);
        cThen = values.get(1);
        cElse = values.get(2);
      } else {
        cond = null;
        cElse = cThen = null;
      }
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
//      createMapping(coll, projectResult, projectedFields, cond, cond, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      String value = extractValue(object, cond).toString();
      String secondValue = extractValue(object, cond).toString();
      int strcmp = compare(value, secondValue);
      result.put(destName, strcmp < 0 ? -1 : strcmp > 1 ? 1 : 0);
    }

    int compare(String value1, String value2) {
      return value1.compareTo(value2);
    }
  }

  static class ProjectedStrcasecmp extends ProjectedCmp {
    static final String KEYWORD = "$strcasecmp";

    public ProjectedStrcasecmp(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, coll, object);
    }

    @Override
    protected int compare(String value1, String value2) {
      return value1.compareToIgnoreCase(value2);
    }
  }

  static class ProjectedToLower extends ProjectedAbstract<ProjectedToLower> {
    static final String KEYWORD = "$toLower";

    private final String field;

    public ProjectedToLower(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToLower(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (value instanceof List) {
        List values = (List) value;
        if (values.size() != 1) {
          errorResult(coll, 16020, "the " + keyword + " operator requires 1 operand(s)");
        }
        field = (String) values.get(0);
      } else {
        field = value.toString();
      }
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field, field, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object value = extractValue(object, field);
      if (value == null) {
        value = "";
      } else {
        value = transformValue(value.toString());
      }
      result.put(destName, value);
    }

    String transformValue(String value) {
      return value.toLowerCase();
    }
  }

  static class ProjectedToUpper extends ProjectedToLower {
    static final String KEYWORD = "$toUpper";

    public ProjectedToUpper(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, coll, object);
    }

    @Override
    protected String transformValue(String value) {
      return value.toUpperCase();
    }
  }

  static class ProjectedToDivide extends ProjectedAbstract<ProjectedToDivide> {
    static final String KEYWORD = "$divide";


    private final Object expression1;
    private final Object expression2;

    public ProjectedToDivide(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToDivide(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array of 2 operands");
      }
      List values = (List) value;
      expression1 = values.get(0);
      expression2 = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
//      createMapping(coll, projectResult, projectedFields, destName, destName, namespace, this);
      createMapping(coll, projectResult, projectedFields, destName, expression1, namespace, this);
      createMapping(coll, projectResult, projectedFields, destName, expression2, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      final Number left = extractValue(object, expression1);
      final Number right = extractValue(object, expression2);
      result.put(destName, Util.genericDiv(left, right));
    }
  }

  static class ProjectedToMod extends ProjectedAbstract<ProjectedToMod> {
    static final String KEYWORD = "$mod";

    private final Object expression1;
    private final Object expression2;

    public ProjectedToMod(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToMod(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array of 2 operands");
      }
      List values = (List) value;
      expression1 = values.get(0);
      expression2 = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, destName, expression1, namespace, this);
      createMapping(coll, projectResult, projectedFields, destName, expression2, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      final Number left = extractValue(object, expression1);
      final Number right = extractValue(object, expression2);
      result.put(destName, Util.genericMod(left, right));
    }
  }

  static class ProjectedToMultiply extends ProjectedAbstract<ProjectedToMultiply> {
    static final String KEYWORD = "$multiply";


    private final Object expression1;
    private final Object expression2;

    public ProjectedToMultiply(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToMultiply(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array of 2 operands");
      }
      List values = (List) value;
      expression1 = values.get(0);
      expression2 = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, destName, expression1, namespace, this);
      createMapping(coll, projectResult, projectedFields, destName, expression2, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      final Number left = extractValue(object, expression1);
      final Number right = extractValue(object, expression2);
      result.put(destName, Util.genericMul(left, right));
    }
  }

  static class ProjectedToAdd extends ProjectedAbstract<ProjectedToAdd> {
    static final String KEYWORD = "$add";


    private final List<Object> expressions;

    public ProjectedToAdd(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToAdd(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() < 1) {
        errorResult(coll, 16020, "the " + keyword + " operator requires an array with at least 1 operand");
      }
      expressions = (List) value;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      for (Object expression : expressions) {
        createMapping(coll, projectResult, projectedFields, destName, expression, namespace, this);
      }
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Number add = extractValue(object, expressions.get(0));
      if (expressions.size() > 1) {
        for (int i = 1; i < expressions.size(); i++) {
          add = Util.genericAdd(add, (Number) extractValue(object, expressions.get(i)));
        }
      }
      result.put(destName, add);
    }
  }

  static class ProjectedToSubtract extends ProjectedAbstract<ProjectedToSubtract> {
    static final String KEYWORD = "$subtract";


    private final List<Object> expressions;

    public ProjectedToSubtract(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedToSubtract(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "Expression " + keyword + " takes exactly 2 arguments.");
      }
      expressions = (List) value;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      for (Object expression : expressions) {
        createMapping(coll, projectResult, projectedFields, destName, expression, namespace, this);
      }
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Number add = extractValue(object, expressions.get(0));
      for (int i = 1; i < expressions.size(); i++) {
        add = Util.genericSub(add, (Number) extractValue(object, expressions.get(i)));
      }
      result.put(destName, add);
    }
  }

  static abstract class ProjectedDate<T extends ProjectedDate> extends ProjectedAbstract<T> {

    private final String field;
    private final int fromCalendar; // See Calendar.*
    private final int modifier; // See Calendar.*

    public ProjectedDate(String keyword, int fromCalendar, int modifier, String destName, DBCollection coll, DBObject object) {
      super(keyword, destName, object);
      Object value = object.get(keyword);
      this.fromCalendar = fromCalendar;
      this.modifier = modifier;
      if (value instanceof List) {
        List list = (List) value;
        if (list.size() != 1) {
          errorResult(coll, 16020, "Expression " + keyword + " takes exactly 1 arguments. " + list.size() + " were passed in.");
        }
        value = list.get(0);
      }
      if (!(value instanceof String)) {
        errorResult(coll, 16020, "the " + keyword + " operator requires a field name");
      }
      this.field = (String) value;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field, field, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object value = extractValue(object, field);
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ENGLISH);
      calendar.setTimeInMillis((((Date) value).getTime()));
      int extracted = calendar.get(fromCalendar) + modifier;
      result.put(destName, extracted);
    }
  }

  // http://docs.mongodb.org/manual/reference/operator/aggregation-date/
  static class ProjectedDateDayOfYear extends ProjectedDate<ProjectedDateDayOfYear> {
    public static final String KEYWORD = "$dayOfYear";

    public ProjectedDateDayOfYear(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.DAY_OF_YEAR, 0, destName, coll, object);
    }
  }

  static class ProjectedDateDayOfMonth extends ProjectedDate<ProjectedDateDayOfMonth> {
    public static final String KEYWORD = "$dayOfMonth";

    public ProjectedDateDayOfMonth(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.DAY_OF_MONTH, 0, destName, coll, object);
    }
  }

  static class ProjectedDateDayOfWeek extends ProjectedDate<ProjectedDateDayOfWeek> {
    public static final String KEYWORD = "$dayOfWeek";

    public ProjectedDateDayOfWeek(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.DAY_OF_WEEK, 0, destName, coll, object);
    }
  }

  static class ProjectedDateYear extends ProjectedDate<ProjectedDateYear> {
    public static final String KEYWORD = "$year";

    public ProjectedDateYear(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.YEAR, 0, destName, coll, object);
    }
  }

  static class ProjectedDateMonth extends ProjectedDate<ProjectedDateMonth> {
    public static final String KEYWORD = "$month";

    public ProjectedDateMonth(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.MONTH, 1, destName, coll, object);
    }
  }

  static class ProjectedDateWeek extends ProjectedDate<ProjectedDateWeek> {
    public static final String KEYWORD = "$week";

    public ProjectedDateWeek(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.WEEK_OF_YEAR, -1, destName, coll, object);
    }
  }

  static class ProjectedDateHour extends ProjectedDate<ProjectedDateHour> {
    public static final String KEYWORD = "$hour";

    public ProjectedDateHour(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.HOUR_OF_DAY, 0, destName, coll, object);
    }
  }

  static class ProjectedDateMinute extends ProjectedDate<ProjectedDateMinute> {
    public static final String KEYWORD = "$minute";

    public ProjectedDateMinute(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.MINUTE, 0, destName, coll, object);
    }
  }

  static class ProjectedDateSecond extends ProjectedDate<ProjectedDateSecond> {
    public static final String KEYWORD = "$second";

    public ProjectedDateSecond(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.SECOND, 0, destName, coll, object);
    }
  }

  static class ProjectedDateMillisecond extends ProjectedDate<ProjectedDateMillisecond> {
    public static final String KEYWORD = "$millisecond";

    public ProjectedDateMillisecond(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, Calendar.MILLISECOND, 0, destName, coll, object);
    }
  }

  /**
   * Simple {@see http://docs.mongodb.org/manual/reference/aggregation/project/#pipe._S_project}
   */
  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    LOG.debug("project() : {}", object);

    DBObject project = ExpressionParser.toDbObject(object.get(getKeyword()));
    DBObject projectResult = Util.clone(project);

    // Extract fields who will be renamed.
    Map<String, List<ProjectedAbstract>> projectedFields = new HashMap<String, List<ProjectedAbstract>>();
    for (Map.Entry<String, Object> entry : Util.entrySet(project)) {
      if (entry.getValue() != null) {
        ProjectedAbstract.createMapping(coll, projectResult, projectedFields, entry.getKey(), entry.getValue(), "", ProjectedRename.newInstance(entry.getKey(), coll, null));
      }
    }

    LOG.debug("project() of {} renamed {}", projectResult, projectedFields);
    List<DBObject> objects = coll.find(null, projectResult).toArray();

    // Rename or transform fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = new BasicDBObject(FongoDBCollection.ID_FIELD_NAME, result.get(FongoDBCollection.ID_FIELD_NAME));
      for (Map.Entry<String, List<ProjectedAbstract>> entry : projectedFields.entrySet()) {
        if (Util.containsField(result, entry.getKey())) {
          for (ProjectedAbstract projected : entry.getValue()) {
            projected.unapply(renamed, result, entry.getKey());
          }
        }
      }

      // TODO REFACTOR
      // Handle special case like ifNull who can doesn't have field in list.
      for (List<ProjectedAbstract> projecteds : projectedFields.values()) {
        for (ProjectedAbstract projected : projecteds) {
//        if (!projected.isDone() && (projected.keyword.recallIfNotFound)) {
          projected.unapply(renamed, result, null);
//        }
//        projected.setDone(false);
        }
      }

      objectsResults.add(renamed);
    }
    coll = dropAndInsert(coll, objectsResults);
    LOG.debug("project() : {}, result : {}", object, objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$project";
  }


  public static class ProjectedFilter extends ProjectedAbstract<ProjectedFilter> {
    public static final String KEYWORD = "$filter";

    private static final String INPUT = "input";
    private static final String COND = "cond";
    private static final String ITEMS = "items";
    private static final String AS = "as";

    public ProjectedFilter(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    ProjectedFilter(String keyword, String destName, DBCollection coll, DBObject pipeline) {
      super(KEYWORD, destName, pipeline);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {

      Object items = result.get(ITEMS);
      if (items == null && key == null) {
        result.put(ITEMS, null);
        return;
      }

      BasicDBObject basicDBObject = (BasicDBObject) object;
      BasicDBList elementToAdd = (BasicDBList) basicDBObject.get(key);
      if (elementToAdd == null)
        return;

      result.put(ITEMS, elementToAdd);
    }


    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String items_key, Object pipeline, String namespace) {
      BasicDBObject pipelineOasicDBObject = (BasicDBObject) pipeline;

      String input = getInput(pipelineOasicDBObject);

      BasicDBObject queryToDelete = new BasicDBObject("$pull", new BasicDBObject(input, projectQuery(new ArrayList<BasicDBObject>(), new BasicDBObject(), getAs(pipelineOasicDBObject), getCond(pipelineOasicDBObject))));

      DBCollection tmpCollection = getClonedCollection(coll);

      tmpCollection.updateMulti(new BasicDBObject(), queryToDelete);

      subtraction(input, tmpCollection, coll);

      createMapping(coll, projectResult, projectedFields, INPUT, input, namespace, this);
    }


    /**
     * This method creates a new DBCollection with the elements of the DBCollection passed like parameter
     *
     * @param coll
     * @return a copy of the DBCollection
     */
    private DBCollection getClonedCollection(DBCollection coll) {

      DBCollection tmpCollection = new FongoDBCollection((FongoDB) coll.getDB(), "tmp");
      DBCursor cursor = coll.find();

      while (cursor.hasNext())
        tmpCollection.insert(cursor.next());

      return tmpCollection;
    }

    private String getInput(BasicDBObject pipelineOasicDBObject) {
      String input = pipelineOasicDBObject.getString(INPUT);
      return (input.startsWith("$")) ? input.substring(1, input.length())
          : input;
    }

    private String getAs(BasicDBObject pipelineOasicDBObject) {
      return pipelineOasicDBObject.getString(AS);
    }

    private BasicDBObject getCond(BasicDBObject pipelineOasicDBObject) {
      return (BasicDBObject) pipelineOasicDBObject.get(COND);
    }


    private boolean isLeaf(Object element) {
      return !(element instanceof BasicDBObject);
    }


    /**
     * This method creates a query to pass at a collection. This query will remove the element which DOESN'T match with the piupeline condition
     *
     * @param as
     * @param cond
     * @return
     */
    private BasicDBObject projectQuery(List<BasicDBObject> leaves, BasicDBObject result, String as, BasicDBObject cond) {

      if (cond.isEmpty())
        return new BasicDBObject();

      for (String function : cond.keySet()) {

        BasicDBList functionParameters = (BasicDBList) cond.get(function);

        if (functionParameters instanceof List) {

          for (Object ele : functionParameters) {

            if (isLeaf(ele))
              return getLeafBasicDBObject(ele, as, functionParameters, function);
            else {
              BasicDBObject leaf = projectQuery(leaves, result, as, (BasicDBObject) ele);
              leaves.addAll(Arrays.asList(leaf));
              result.append(function, leaves);
            }

          }
        }
      }
      return result;

    }

    private BasicDBObject getLeafBasicDBObject(Object ele, String as, BasicDBList functionParameters, String function) {

      if (!(ele instanceof String))
        return new BasicDBObject();

      String[] functionValue = ((String) ele).split("\\.");

      String alias = functionValue[0];

      if (!alias.replaceAll("\\$", "").equals(as))
        throw new IllegalArgumentException("Use of undefined variable: " + alias.replaceAll("\\$", ""));

      String valueToApplay = functionValue[1];
      Object value = functionParameters.get(1);
      BasicDBObject condition = new BasicDBObject(function, value);
      BasicDBObject result = new BasicDBObject(valueToApplay, condition);
      return result;


    }

    private void subtraction(String input, DBCollection sourceCollection, DBCollection targetCollection) {

      DBCursor cursor = sourceCollection.find();
      while (cursor.hasNext()) {

        DBObject document = (DBObject) cursor.next();
        BasicDBList arrayEmelents = (BasicDBList) document.get(input);

        if (arrayEmelents != null) {
          for (int i = 0; i <= arrayEmelents.size() - 1; i++) {
            BasicDBObject arrayObjectToDelete = (BasicDBObject) arrayEmelents.get(i);
            targetCollection.updateMulti(new BasicDBObject(), new BasicDBObject("$pull", new BasicDBObject(input, arrayObjectToDelete)));

          }
        }

      }

    }

  }


  public static class ProjectedOr extends ProjectedAbstract<ProjectedOr> {
    public static final String KEYWORD = "$or";

    private List<String> fromLocations = null;

    public ProjectedOr(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object from = object.get(keyword);

      if (from instanceof List) {
        fromLocations = (List<String>) from;
      } else if (from instanceof String) {
        fromLocations = new ArrayList<String>();
        fromLocations.add((String) from);
      }

    }



    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      for (String fromLocation : fromLocations) {
        if (fromLocation.startsWith("$")) {
          fromLocation = fromLocation.substring(1);
        }

        BasicDBList list = (BasicDBList) object.get(fromLocation);

        Boolean orValue = false;
        for (Object o : list) {
          if (o instanceof Boolean) {
            Boolean current = (Boolean) o;
            orValue = current || orValue;

            if (orValue) {
              break;
            }
          }
        }

        result.put(destName, orValue);
      }

    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      for (String fromLocation : fromLocations) {
        createMapping(coll, projectResult, projectedFields, destName, fromLocation, namespace, this);    }
      }
  }

  public static class ProjectedAnd extends ProjectedAbstract<ProjectedAnd> {
    public static final String KEYWORD = "$and";

    private List<String> fromLocations = null;

    public ProjectedAnd(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object from = object.get(keyword);

      if (from instanceof List) {
        fromLocations = (List<String>) from;
      } else if (from instanceof String) {
        fromLocations = new ArrayList<String>();
        fromLocations.add((String) from);
      }

    }



    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      for (String fromLocation : fromLocations) {
        if (fromLocation.startsWith("$")) {
          fromLocation = fromLocation.substring(1);
        }

        BasicDBList list = (BasicDBList) object.get(fromLocation);

        Boolean andValue = true;
        for (Object o : list) {
          if (o instanceof Boolean) {
            Boolean current = (Boolean) o;
            andValue = current && andValue;

            if (!andValue) {
              break;
            }
          }
        }

        result.put(destName, andValue);
      }

    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, List<ProjectedAbstract>> projectedFields, String key, Object value, String namespace) {
      for (String fromLocation : fromLocations) {
        createMapping(coll, projectResult, projectedFields, destName, fromLocation, namespace, this);    }
      }
  }

}
