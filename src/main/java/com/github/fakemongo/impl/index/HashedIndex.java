package com.github.fakemongo.impl.index;

import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.LinkedHashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index for the MongoDB.
 */
public class HashedIndex extends IndexAbstract<DBObject> {
  private static final Logger LOG = LoggerFactory.getLogger(HashedIndex.class);

  HashedIndex(String name, DBObject keys, boolean unique, String hashed, boolean sparse) {
    super(name, keys, unique, new LinkedHashMap<DBObject, IndexedList<DBObject>>(), hashed, sparse);
  }

  /**
   * Create the key for the hashmap.
   *
   * @param object
   * @return
   */
  @Override
  protected DBObject getKeyFor(DBObject object) {
    return object;
  }

  @Override
  public DBObject embedded(DBObject object) {
    return object;
  }

  @Override
  public List<List<Object>> addOrUpdate(DBObject object, DBObject oldObject) {
    if (object.get(this.geoIndex) instanceof List) {
      throw new MongoException(16766, "Error: hashed indexes do not currently support array values");
    }
    return super.addOrUpdate(object, oldObject);
  }
}