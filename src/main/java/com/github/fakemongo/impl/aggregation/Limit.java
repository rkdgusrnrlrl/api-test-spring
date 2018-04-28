package com.github.fakemongo.impl.aggregation;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.annotations.ThreadSafe;
import java.util.List;

/**
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Limit extends PipelineKeyword {
  public static final Limit INSTANCE = new Limit();

  private Limit() {
  }

  /**
   */
  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    List<DBObject> objects = coll.find().limit(((Number) object.get(getKeyword())).intValue()).toArray();
    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$limit";
  }

}
