package com.github.fakemongo.impl;

import com.github.fakemongo.Fongo;
import com.mongodb.*;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.util.FongoJSON;
import com.mongodb.util.ObjectSerializer;
import org.mozilla.javascript.*;
import org.mozilla.javascript.tools.shell.Global;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
 * <p/>
 * TODO : finalize.
 */
public class MapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class);

  private final FongoDB fongoDB;

  private final FongoDBCollection fongoDBCollection;

  private final String map;

  private final String reduce;

  // TODO
  private final String finalize;

  private final Map<String, Object> scope;

  private final DBObject out;

  private final DBObject query;

  private final DBObject sort;

  private final int limit;

  // http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
  private enum Outmode {
    REPLACE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        coll.insert(results);
      }
    },
    MERGE {
      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        // Upsert == insert the result if not exist.
        for (DBObject result : results) {
          coll.update(new BasicDBObject(FongoDBCollection.ID_FIELD_NAME, result.get(FongoDBCollection.ID_FIELD_NAME)), result, true,
              false);
        }
      }
    },
    REDUCE {
      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        final List<DBObject> reduced = mr.reduceOutputStage(coll, results);
        for (DBObject result : reduced) {
          coll.update(new BasicDBObject(FongoDBCollection.ID_FIELD_NAME, result.get(FongoDBCollection.ID_FIELD_NAME)), result, true,
              false);
        }
      }
    },
    INLINE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        coll.insert(results);
      }

      @Override
      public String collectionName(DBObject object) {
        // Random uuid for extract result after.
        return UUID.randomUUID().toString();
      }

      @Override
      public MapReduceOutput createResult(DBObject query, final DBCollection coll, final MapReduceStatistics ignored) {
        return new FongoMapReduceOutput(query, coll.find().toArray());
      }
    };

    public static Outmode valueFor(DBObject object) {
      for (Outmode outmode : values()) {
        if (object.containsField(outmode.name().toLowerCase())) {
          return outmode;
        }
      }
      return null;
    }

    public static Outmode valueFor(MapReduceCommand.OutputType outputType) {
      for (Outmode outmode : values()) {
        if (outputType.name().equalsIgnoreCase(outmode.name().toLowerCase())) {
          return outmode;
        }
      }
      return null;
    }

    public String collectionName(DBObject object) {
      return (String) object.get(name().toLowerCase());
    }

    public void initCollection(DBCollection coll) {
      // Do nothing.
    }

    public abstract void newResults(MapReduce mapReduce, DBCollection coll, List<DBObject> results);

    public MapReduceOutput createResult(final DBObject query, final DBCollection coll, final MapReduceStatistics mapReduceStatistics) {
      return new FongoMapReduceOutput(query, coll, mapReduceStatistics);
    }

  }

  public MapReduce(Fongo fongo, FongoDBCollection coll, String map, String reduce, String finalize,
                   Map<String, Object> scope, DBObject out, DBObject query, DBObject sort, Number limit) {
    if (out.containsField("db")) {
      this.fongoDB = fongo.getDB((String) out.get("db"));
    } else {
      this.fongoDB = (FongoDB) coll.getDB();
    }
    this.fongoDBCollection = coll;
    this.map = map;
    this.reduce = reduce;
    this.finalize = finalize;
    this.scope = scope;
    this.out = out;
    this.query = query;
    this.sort = sort;
    this.limit = limit == null ? 0 : limit.intValue();
  }

  /**
   * @return null if error.
   */
  public MapReduceOutput computeResult() {
    final long startTime = System.currentTimeMillis();
    // Replace, merge or reduce ?
    Outmode outmode = Outmode.valueFor(out);
    DBCollection coll = fongoDB.getCollection(outmode.collectionName(out));
    // Mode replace.
    outmode.initCollection(coll);
    final MapReduceResult mapReduceResult = runInContext();
    outmode.newResults(this, coll, mapReduceResult.result);

    final MapReduceStatistics mapReduceStatistics = new MapReduceStatistics(mapReduceResult.inputCount, mapReduceResult.outputCount, mapReduceResult.emitCount, (int) (System.currentTimeMillis() - startTime));
    final MapReduceOutput result = outmode.createResult(this.query, coll, mapReduceStatistics);
    LOG.debug("computeResult() : {}", result);
    return result;
  }

  static class MapReduceResult {
    final int inputCount, outputCount, emitCount;
    final List<DBObject> result;

    public MapReduceResult(int inputCount, int outputCount, int emitCount, List<DBObject> result) {
      this.inputCount = inputCount;
      this.outputCount = outputCount;
      this.emitCount = emitCount;
      this.result = result;
    }
  }

  private MapReduceResult runInContext() {
    // TODO use Compilable ? http://www.jmdoudoux.fr/java/dej/chap-scripting.htm
    Context cx = Context.enter();
    try {
      Scriptable scriptable = new Global(cx);//cx.initStandardObjects();
      cx.initStandardObjects();
      ScriptableObject.defineClass(scriptable, FongoNumberLong.class);
      ScriptableObject.defineClass(scriptable, FongoNumberInt.class);
//      cx.setGeneratingDebug(true);
//      cx.getWrapFactory().setJavaPrimitiveWrap(false);

      final StringBuilder stringBuilder = new StringBuilder();
      // Add some function to javascript engine.
      this.addMongoFunctions(stringBuilder);
      this.addScopeObjects(stringBuilder);

      final List<String> javascriptFunctions = new ArrayList<String>();
      javascriptFunctions.add(stringBuilder.toString());
      final List<DBObject> objects = this.fongoDBCollection.find(query).sort(sort).limit(limit).toArray();
      constructJavascriptFunction(javascriptFunctions, objects);
      for (String jsFunction : javascriptFunctions) {
        try {
          cx.evaluateString(scriptable, jsFunction, "MapReduce", 0, null);
        } catch (RhinoException e) {
          LOG.error("Exception running script {}", jsFunction, e);
          if (e.getMessage().contains("FongoAssertException")) {
            fongoDB.notOkErrorResult(16722, "Error: assert failed: " + e.getMessage()).throwOnError();
          }
          fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
        }
      }

      // Get the result into an object.
      final NativeArray outs = (NativeArray) scriptable.get("$$$fongoOuts$$$", scriptable);
      final List<DBObject> dbOuts = new ArrayList<DBObject>();
      for (int i = 0; i < outs.getLength(); i++) {
        final NativeObject out = (NativeObject) outs.get(i, outs);
        dbOuts.add(getObject(out));
      }
      // TODO : verify emitCount
      return new MapReduceResult(objects.size(), dbOuts.size(), objects.size(), dbOuts);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      Context.exit();
    }
  }

  private void addScopeObjects(StringBuilder stringBuilder) {
    if (this.scope != null) {
      for (Map.Entry<String, Object> entry : this.scope.entrySet()) {
        stringBuilder.append("var ").append(entry.getKey()).append(" = ");
        FongoJSON.serialize(entry.getValue(), stringBuilder, OBJECT_SERIALIZERS);
        stringBuilder.append(";\n");
      }
    }
  }

  private List<DBObject> reduceOutputStage(DBCollection coll, List<DBObject> mapReduceOutput) {
    Context cx = Context.enter();
    try {
      final Scriptable scope = cx.initStandardObjects();
      final List<String> jsFunctions = constructReduceOutputStageJavascriptFunction(coll, mapReduceOutput);
      for (String jsFunction : jsFunctions) {
        try {
          cx.evaluateString(scope, jsFunction, "<reduce output stage>", 0, null);
        } catch (RhinoException e) {
          LOG.error("Exception running script {}", jsFunction, e);
          if (e.getMessage().contains("FongoAssertException")) {
            fongoDB.notOkErrorResult(16722, "Error: assert failed: " + e.getMessage()).throwOnError();
          }
          fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
        }
      }

      // Get the result into an object.
      NativeArray outs = (NativeArray) scope.get("$$$fongoOuts$$$", scope);
      List<DBObject> dbOuts = new ArrayList<DBObject>();
      for (int i = 0; i < outs.getLength(); i++) {
        NativeObject out = (NativeObject) outs.get(i, outs);
        dbOuts.add(getObject(out));
      }

      LOG.debug("reduceOutputStage() : {}", dbOuts);
      return dbOuts;
    } finally {
      Context.exit();
    }
  }


  DBObject getObject(ScriptableObject no) {
    if (no instanceof NativeArray) {
      BasicDBList ret = new BasicDBList();
      NativeArray noArray = (NativeArray) no;
      for (int i = 0; i < noArray.getLength(); i++) {
        Object value = noArray.get(i, noArray);
        value = getObjectOrTransform(value);
        ret.add(value);
      }
      return ret;
    }
    DBObject ret = new BasicDBObject();
    Object[] propIds = no.getIds();
    for (Object propId : propIds) {
      String key = Context.toString(propId);
      Object value = NativeObject.getProperty(no, key);
      value = getObjectOrTransform(value);
      ret.put(key, value);
    }
    return ret;
  }

  private Object getObjectOrTransform(Object value) {
    if (value instanceof NativeObject || value instanceof NativeArray) {
      value = getObject((ScriptableObject) value);
    }
    if (value instanceof Integer) {
      value = ((Integer) value).doubleValue();
    }
    if (value instanceof ConsString) {
      value = value.toString();
    }
    if (value instanceof NativeJavaObject) {
      value = ((NativeJavaObject) value).unwrap();
    }
    if (value instanceof FongoNumberLong) {
      value = ((FongoNumberLong) value).value;
    }
    if (value instanceof FongoNumberInt) {
      value = ((FongoNumberInt) value).value;
    }
    return value;
  }

  /**
   * Create the map/reduce/finalize function.
   */
  private List<String> constructJavascriptFunction(List<String> result, List<DBObject> objects) {
    StringBuilder sb = new StringBuilder(80000);

    // Create variables for exporting.
    sb.append("var $$$fongoEmits$$$ = new Object();\n");
    sb.append("function emit(param1, param2) {\n" +
        "var toSource = param1.toSource();\n" +
        "if(typeof $$$fongoEmits$$$[toSource] === 'undefined') {\n " +
        "$$$fongoEmits$$$[toSource] = new Array();\n" +
        "}\n" +
        "var val = {id: param1, value: param2};\n" +
        "$$$fongoEmits$$$[toSource][$$$fongoEmits$$$[toSource].length] = val;\n" +
        "};\n");
    // Prepare map function.
    sb.append("var fongoMapFunction = ").append(map).append(";\n");
    sb.append("var $$$fongoVars$$$ = new Object();\n");
    // For each object, execute in javascript the function.
    for (DBObject object : objects) {
      sb.append("$$$fongoVars$$$ = ");
      FongoJSON.serialize(object, sb, OBJECT_SERIALIZERS);
      sb.append(";\n");
      sb.append("$$$fongoVars$$$['fongoExecute'] = fongoMapFunction;\n");
      sb.append("$$$fongoVars$$$.fongoExecute();\n");
//      if (sb.length() > 65535) { // Rhino limit :-(
//        result.add(sb.toString());
//        sb.setLength(0);
//      }
    }
    result.add(sb.toString());

    // Add Reduce Function
    sb.setLength(0);
    sb.append("var reduce = ").append(reduce).append("\n");
    sb.append("var $$$fongoOuts$$$ = Array();\n" +
        "for(var i in $$$fongoEmits$$$) {\n" +
        "var elem = $$$fongoEmits$$$[i];\n" +
        "var values = []; id = null; for (var ii in elem) { values.push(elem[ii].value); id = elem[ii].id;}\n" +
        "$$$fongoOuts$$$[$$$fongoOuts$$$.length] = { _id : id, value : reduce(id, values) };\n" +
        "}\n");
    result.add(sb.toString());

    return result;
  }

  /**
   * Create 'reduce' stage output function.
   */
  private List<String> constructReduceOutputStageJavascriptFunction(DBCollection coll, List<DBObject> objects) {
    List<String> result = new ArrayList<String>();
    StringBuilder sb = new StringBuilder(80000);

    addMongoFunctions(sb);

    sb.append("var reduce = ").append(reduce).append("\n");
    sb.append("var $$$fongoOuts$$$ = new Array();\n");
    for (DBObject object : objects) {
      String objectJson = FongoJSON.serialize(object);
      String objectValueJson = FongoJSON.serialize(object.get("value"));
      DBObject existing = coll.findOne(new BasicDBObject().append(FongoDBCollection.ID_FIELD_NAME,
          object.get(FongoDBCollection.ID_FIELD_NAME)));
      if (existing == null || existing.get("value") == null) {
        sb.append("$$$fongoOuts$$$[$$$fongoOuts$$$.length] = ").append(objectJson).append(";\n");
      } else {
        String id = FongoJSON.serialize(object.get(FongoDBCollection.ID_FIELD_NAME));
        String existingValueJson = FongoJSON.serialize(existing.get("value"));
        sb.append("$$$fongoId$$$ = ").append(id).append(";\n");
        sb.append("$$$fongoValues$$$ = [ ").append(existingValueJson).append(", ").append(objectValueJson).append("];\n");
        sb.append("$$$fongoReduced$$$ = { _id: $$$fongoId$$$, 'value': reduce($$$fongoId$$$, $$$fongoValues$$$)};")
            .append(";\n");
        sb.append("$$$fongoOuts$$$[$$$fongoOuts$$$.length] = $$$fongoReduced$$$;\n");
      }
      if (sb.length() > 65535) { // Rhino limit :-(
        result.add(sb.toString());
        sb.setLength(0);
      }
    }
    result.add(sb.toString());
    return result;
  }

  private void addMongoFunctions(StringBuilder construct) {
    // Add some function to javascript engine.
    construct.append("Array.sum = function(array) {\n" +
        "    var a = 0;\n" +
        "    for (var i = 0; i < array.length; i++) {\n" +
        "        a = a + array[i];\n" +
        "    }\n" +
        "    return a;" +
        "};\n");

    construct.append("printjson = function(a) {" +
        "    print(tojson(a));\n" +
        " };\n");

    construct.append("printjsononeline = function(a) {\n" +
        "    print(tojson(a));\n" +
        " };\n");

    construct.append("assert = function(a) {\n" +
        "    if (!a) throw new FongoAssertException();\n" +
        " };\n");

    construct.append("isString = function(a) {\n" +
        "    return typeof(a) === 'string';\n" +
        " };\n");

    construct.append("isNumber = function(a) {\n" +
        "    return typeof(a) === 'number';\n" +
        " };\n");

    construct.append("isObject = function(a) {\n" +
        "    return typeof(a) === 'object';\n" +
        " };\n");

    construct.append("tojson = function(a) {\n" +
        "    return JSON.stringify(a,null,0);\n" +
        " };\n");

    construct.append("tojsononeline = function(a) {\n" +
        "    return JSON.stringify(a,null,0);\n" +
        " };\n");

    construct.append("NumberLong = function(a) {\n" +
        "        return new FongoNumberLong(a);\n" +
        "};\n");

    construct.append("NumberInt = function(a) {\n" +
        "        return new FongoNumberInt(a);\n" +
        "};\n");
  }

  public static class FongoNumberLong extends ScriptableObject {
    Long value;

    public FongoNumberLong() {
    }

    // Method jsConstructor defines the JavaScript constructor
    public void jsConstructor(Double a) {
      this.value = a.longValue();
    }

    public double jsFunction_toNumber() {
      return value;
    }

    public double jsFunction_valueOf() {
      return jsFunction_toNumber();
    }

    @Override
    public String getClassName() {
      return "FongoNumberLong";
    }

    public String jsFunction_toString() {
      return "NumberLong(" + this.value + ")";
    }
  }

  public static class FongoNumberInt extends ScriptableObject {
    int value;

    public FongoNumberInt() {
    }

    // Method jsConstructor defines the JavaScript constructor
    public void jsConstructor(int a) {
      this.value = a;
    }

    public int jsFunction_toNumber() {
      return value;
    }

    public int jsFunction_valueOf() {
      return value;
    }

    @Override
    public String getClassName() {
      return "FongoNumberInt";
    }

    public String jsFunction_toString() {
      return "NumberInt(" + this.value + ")";
    }
  }


  private static class FongoLongSerializer implements ObjectSerializer {

    @Override
    public String serialize(final Object obj) {
      StringBuilder builder = new StringBuilder();
      serialize(obj, builder);
      return builder.toString();
    }

    @Override
    public void serialize(final Object obj, final StringBuilder buf) {
      buf.append("NumberLong(").append(obj.toString()).append(")");
    }
  }

  private static class FongoIntegerSerializer implements ObjectSerializer {
    @Override
    public String serialize(final Object obj) {
      StringBuilder builder = new StringBuilder();
      serialize(obj, builder);
      return builder.toString();
    }

    @Override
    public void serialize(final Object obj, final StringBuilder buf) {
      buf.append("NumberInt(").append(obj.toString()).append(")");
    }
  }

  static final Map<Class<?>, ObjectSerializer> OBJECT_SERIALIZERS = new HashMap<Class<?>, ObjectSerializer>();

  static {
    OBJECT_SERIALIZERS.put(Long.class, new FongoLongSerializer());
    OBJECT_SERIALIZERS.put(Integer.class, new FongoIntegerSerializer());
  }
}