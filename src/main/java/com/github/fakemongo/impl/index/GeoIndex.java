package com.github.fakemongo.impl.index;

import com.github.fakemongo.impl.Filter;
import com.github.fakemongo.impl.Util;
import com.github.fakemongo.impl.geo.GeoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.vividsolutions.jts.geom.Geometry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index for the MongoDB.
 * <p/>
 * TODO : more $geometry.
 */
public class GeoIndex extends IndexAbstract<GeoUtil.GeoDBObject> {
  private static final Logger LOG = LoggerFactory.getLogger(GeoIndex.class);

  GeoIndex(String name, DBObject keys, boolean unique, String geoIndex, boolean sparse) {
    super(name, keys, unique, new LinkedHashMap<GeoUtil.GeoDBObject, IndexedList<GeoUtil.GeoDBObject>>(), geoIndex, sparse);
    //TreeMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(new GeoUtil.GeoComparator(geoIndex)), geoIndex);
  }

  /**
   * Create the key for the hashmap.
   */
  @Override
  protected GeoUtil.GeoDBObject getKeyFor(DBObject object) {
    return new GeoUtil.GeoDBObject(super.getKeyFor(object), geoIndex);
  }

  @Override
  public GeoUtil.GeoDBObject embedded(DBObject object) {
    return new GeoUtil.GeoDBObject(object, geoIndex); // Important : do not clone, indexes share objects between them.
  }

  public List<DBObject> geoNear(DBObject query, Geometry geometry, int limit, boolean spherical) {
    lookupCount++;

    LOG.info("geoNear() query:{}, geometry:{}, limit:{}, spherical:{} (mapValues size:{})", query, geometry, limit, spherical, mapValues.size());
    // Filter values
    Filter filterValue = expressionParser.buildFilter(query);

    // Preserve order and remove duplicates.
    LinkedHashSet<DBObject> resultSet = new LinkedHashSet<DBObject>();
    geoNearCoverAll(mapValues, filterValue, geometry, spherical, resultSet);

    return sortAndLimit(resultSet, limit);
  }

  /**
   * Try all the map, without trying to filter by geohash.
   */
  private void geoNearCoverAll(Map<GeoUtil.GeoDBObject, IndexedList<GeoUtil.GeoDBObject>> values, Filter filterValue, Geometry near, boolean spherical, LinkedHashSet<DBObject> resultSet) {
    for (Map.Entry<GeoUtil.GeoDBObject, IndexedList<GeoUtil.GeoDBObject>> entry : values.entrySet()) {
      geoNearResults(entry.getValue().getElements(), filterValue, near, resultSet, spherical);
    }
  }

  /**
   * Sort the results and limit them.
   */
  private List<DBObject> sortAndLimit(Collection<DBObject> resultSet, int limit) {
    List<DBObject> result = new ArrayList<DBObject>(resultSet);
    // Sort values by distance.
    Collections.sort(result, new Comparator<DBObject>() {
      @Override
      public int compare(DBObject o1, DBObject o2) {
        return ((Double) o1.get("dis")).compareTo((Double) o2.get("dis"));
      }
    });
    // Applying limit
    return result.subList(0, Math.min(result.size(), limit));
  }


  // Now transform to {dis:<distance>, obj:<result>}
  private void geoNearResults(List<GeoUtil.GeoDBObject> values, Filter filterValue, Geometry near, Collection<DBObject> result, boolean spherical) {
    for (GeoUtil.GeoDBObject geoDBObject : values) {
      // Test against the query filter.
      if (geoDBObject.getGeometry() != null && filterValue.apply(geoDBObject)) {
        double radians = GeoUtil.distanceInRadians(geoDBObject.getGeometry(), near, spherical);
        geoDBObject.removeField(FongoDBCollection.FONGO_SPECIAL_ORDER_BY);
        result.add(new BasicDBObject("dis", radians).append("obj", Util.clone(geoDBObject)));
      }
    }
  }

}
