package com.github.fakemongo.impl.index;

import com.github.fakemongo.impl.Util;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.Map;

/**
 * A factory for index.
 */
public final class IndexFactory {
  private IndexFactory() {
  }

  public static IndexAbstract create(String name, DBObject keys, boolean unique, boolean sparse) throws MongoException {
    String geoIndex = getGeoKey(keys);
    if (geoIndex != null) {
      return new GeoIndex(name, keys, unique, geoIndex, sparse);
    } else {
      String hashed = getHashedKey(keys);
      if (hashed != null) {
        return new HashedIndex(name, keys, unique, hashed, sparse);
      }
      return new Index(name, keys, unique, sparse);
    }
  }

  private static String getHashedKey(DBObject keys) {
    String hashed = null;
    for (Map.Entry<String, Object> entry : Util.entrySet(keys)) {
      Object value = entry.getValue();
      if (value instanceof String) {
        boolean localHashed = "hashed".equals(value);
        if (localHashed) {
          hashed = entry.getKey();
        }
      }
    }
    return hashed;
  }

  private static String getGeoKey(DBObject keys) {
    boolean first = true;
    String geo = null;
    for (Map.Entry<String, Object> entry : Util.entrySet(keys)) {
      Object value = entry.getValue();
      if (value instanceof String) {
        boolean localGeo = "2d".equals(value) || "2dsphere".equals(value);
        if (localGeo) {
          if (!first && "2d".equals(value)) {
            //	"err" : "2d has to be first in index", "code" : 13023, "n" : 0, "connectionId" : 206, "ok" : 1
            throw new MongoException(13023, "2d has to be first in index");
          }
          geo = entry.getKey();
        }
      }
      first = false;
    }
    return geo;
  }

}
