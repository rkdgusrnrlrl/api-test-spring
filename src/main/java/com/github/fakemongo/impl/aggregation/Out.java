package com.github.fakemongo.impl.aggregation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;

/**
 * User: gdepourtales
 * 2015/06/15
 */
public class Out extends PipelineKeyword {

  public static final Out INSTANCE = new Out();

  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    final List<DBObject> objects = coll.find().toArray();
    DBCollection newCollection = originalDB.getCollection(object.get(getKeyword()).toString());
    // By default, remove all in the collection without dropping indexes.
    newCollection.remove(new BasicDBObject());
    newCollection.insert(objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$out";
  }
}
