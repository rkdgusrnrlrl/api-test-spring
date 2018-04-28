package com.mongodb.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BsonUndefined;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

/**
 *
 */
public class FongoJSONCallback extends BasicBSONCallback {

  @Override
  public BSONObject create() {
    return new BasicDBObject();
  }

  @Override
  protected BSONObject createList() {
    return new BasicDBList();
  }

  @Override
  public void arrayStart(final String name) {
    _lastArray = true;
    super.arrayStart(name);
  }

  @Override
  public void objectStart(final String name) {
    _lastArray = false;
    super.objectStart(name);
  }

  @Override
  public Object objectDone() {
    String name = curName();
    Object o = super.objectDone();
    if (_lastArray) {
      return o;
    }
    BSONObject b = (BSONObject) o;

    // override the object if it's a special type
    if (b.containsField("$oid")) {
      o = new ObjectId((String) b.get("$oid"));
    } else if (b.containsField("$date")) {
      if (b.get("$date") instanceof Number) {
        o = new Date(((Number) b.get("$date")).longValue());
      } else {
        SimpleDateFormat format = new SimpleDateFormat(_msDateFormat);
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        o = format.parse(b.get("$date").toString(), new ParsePosition(0));

        if (o == null) {
          // try older format with no ms
          format = new SimpleDateFormat(_secDateFormat);
          format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
          o = format.parse(b.get("$date").toString(), new ParsePosition(0));
        }
      }
    } else if (b.containsField("$regex")) {
      o = Pattern.compile((String) b.get("$regex"),
          BSON.regexFlags((String) b.get("$options")));
    } else if (b.containsField("$ts")) { //Legacy timestamp format
      Integer ts = ((Number) b.get("$ts")).intValue();
      Integer inc = ((Number) b.get("$inc")).intValue();
      o = new BSONTimestamp(ts, inc);
    } else if (b.containsField("$timestamp")) {
      BSONObject tsObject = (BSONObject) b.get("$timestamp");
      Integer ts = ((Number) tsObject.get("t")).intValue();
      Integer inc = ((Number) tsObject.get("i")).intValue();
      o = new BSONTimestamp(ts, inc);
    } else if (b.containsField("$code")) {
      if (b.containsField("$scope")) {
        o = new CodeWScope((String) b.get("$code"), (DBObject) b.get("$scope"));
      } else {
        o = new Code((String) b.get("$code"));
      }
    } else if (b.containsField("$ref")) {
      o = new DBRef((String) b.get("$ref"), b.get("$id"));
    } else if (b.containsField("$minKey")) {
      o = new MinKey();
    } else if (b.containsField("$maxKey")) {
      o = new MaxKey();
    } else if (b.containsField("$uuid")) {
      o = UUID.fromString((String) b.get("$uuid"));
    } else if (b.containsField("$binary")) {
      Object extracted = b.get("$type");
      int type;
      if (extracted instanceof Integer) {
        type = (Integer) extracted;
      } else {
        type = Integer.valueOf(extracted.toString());
      }
      byte[] bytes = DatatypeConverter.parseBase64Binary((String) b.get("$binary"));
      o = new Binary((byte) type, bytes);
    } else if (b.containsField("$undefined") && b.get("$undefined").equals(true)) {
      o = new BsonUndefined();
    } else if (b.containsField("$numberLong")) {
      o = Long.valueOf((String) b.get("$numberLong"));
    }

    if (!isStackEmpty()) {
      _put(name, o);
    } else {
      o = !BSON.hasDecodeHooks() ? o : BSON.applyDecodingHooks(o);
      setRoot(o);
    }
    return o;
  }

  private boolean _lastArray = false;

  public static final String _msDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String _secDateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";

}
