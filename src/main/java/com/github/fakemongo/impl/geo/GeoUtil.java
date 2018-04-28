package com.github.fakemongo.impl.geo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.FongoJSON;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.geojson.GeoJsonObject;
import org.geojson.LngLatAlt;
import org.geojson.MultiPolygon;
import org.geojson.MultiPoint;
import org.geojson.Point;
import org.geojson.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GeoUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GeoUtil.class);

  /**
   * Hack tricks to be sure than all is ok.
   */
  public static boolean illegalForUnknownGeometry = false;

  //  public static final double EARTH_RADIUS = 6374892.5; // common way : 6378100D;
  // From MongoDB Sources (src/mongo/db/geo/geoconstants.h)
  public static final double EARTH_RADIUS = 6378100d; // common way : 6378100D;
  /**
   * Length (in meters) of one degree.
   */
  public static final double METERS_PER_DEGREE = 111185.0;

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private GeoUtil() {
  }

  public static class GeoDBObject extends BasicDBObject {
    private final Geometry geometry;

    public GeoDBObject(DBObject object, String indexKey) {
      final Object coordinates = Util.extractField(object, indexKey);
      this.geometry = GeoUtil.toGeometry(coordinates);
      this.putAll(object);

      if (geometry == null) {
        LOG.warn("Can't extract geometry from this indexKey :{} (object:{}), coordinates:{}", indexKey, object, coordinates);

        throw new MongoException(16755, "insertDocument :: caused by :: 16755 Can't extract geo keys from object, malformed geometry?: " + FongoJSON.serialize(object));
      }
    }

    public Geometry getGeometry() {
      return geometry;
    }

    @Override
    public int hashCode() {
      return geometry.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoDBObject)) return false;

      GeoDBObject that = (GeoDBObject) o;

      return geometry.equals(that.geometry);
    }

    @Override
    public String toString() {
      return "GeoDBObject{" +
          "geometry='" + geometry + '\'' +
          '}';
    }
  }

  public static boolean geowithin(Geometry p1, Geometry geometry) {
    return DistanceOp.isWithinDistance(p1, geometry, 0D);
  }

  public static com.vividsolutions.jts.geom.Point createGeometryPoint(Coordinate coordinate) {
    return GEOMETRY_FACTORY.createPoint(coordinate);
  }

  public static double distanceInRadians(Geometry p1, Geometry p2, boolean spherical) {
    final Coordinate[] coordinates = DistanceOp.nearestPoints(p1, p2);

    return GeoUtil.distanceInRadians(coordinates[0], coordinates[1], spherical);
  }

  public static double distanceInRadians(Coordinate p1, Coordinate p2, boolean spherical) {
    double distance;
    if (spherical) {
      distance = distanceSpherical(p1, p2);
    } else {
      distance = distance2d(p1, p2);
    }
    return distance;
  }

  // Take me a day before I see this : https://github.com/mongodb/mongo/blob/ba239918c950c254056bf589a943a5e88fd4144c/src/mongo/db/geo/shapes.cpp
  public static double distance2d(Coordinate p1, Coordinate p2) {
    double a = p1.x - p2.x;
    double b = p1.y - p2.y;

    // Avoid numerical error if possible...
    if (a == 0) return Math.abs(b);
    if (b == 0) return Math.abs(a);

    return Math.sqrt((a * a) + (b * b));
  }

  public static double distanceSpherical(Coordinate p1, Coordinate p2) {
    double p1lat = Math.toRadians(p1.x); // e
    double p1long = Math.toRadians(p1.y);    // f
    double p2lat = Math.toRadians(p2.x);         // g
    double p2long = Math.toRadians(p2.y);             // h

    double sinx1 = Math.sin(p1lat), cosx1 = Math.cos(p1lat);
    double siny1 = Math.sin(p1long), cosy1 = Math.cos(p1long);
    double sinx2 = Math.sin(p2lat), cosx2 = Math.cos(p2lat);
    double siny2 = Math.sin(p2long), cosy2 = Math.cos(p2long);

    double crossProduct = cosx1 * cosx2 * cosy1 * cosy2 + cosx1 * siny1 * cosx2 * siny2 + sinx1 * sinx2;
    if (crossProduct >= 1D || crossProduct <= -1D) {
      return crossProduct > 0 ? 0 : Math.PI;
    }

    return Math.acos(crossProduct);
  }

  /**
   * Retrieve LatLon from an object.
   * <p/>
   * Object can be:
   * - [lon, lat]
   * - {lat:lat, lng:lon}
   */
  public static List<Coordinate> coordinate(List<String> path, DBObject object) {
    ExpressionParser expressionParser = new ExpressionParser();
    List<Coordinate> result = new ArrayList<Coordinate>();

    List objects;
    if (path.isEmpty()) {
      objects = Collections.singletonList(object);
    } else {
      objects = expressionParser.getEmbeddedValues(path, object);
    }
    for (Object value : objects) {
      Coordinate coordinate = coordinate(value);
      if (coordinate != null) {
        result.add(coordinate);
      }
    }
    return result;
  }

  public static Coordinate coordinate(Object value) {
    Coordinate coordinate = null;
    if (value instanceof List) {
      List list = (List) value;
      if (list.size() == 2) {
        coordinate = new Coordinate(((Number) list.get(1)).doubleValue(), ((Number) list.get(0)).doubleValue());
      } else {
        LOG.warn("Strange, coordinate of {} has not a size of 2", value);
      }
    } else if (ExpressionParser.isDbObject(value)) {
      DBObject dbObject = ExpressionParser.toDbObject(value);
      if (dbObject.containsField("type")) {
        // GeoJSON
        try {
          GeoJsonObject object = new ObjectMapper().readValue(FongoJSON.serialize(value), GeoJsonObject.class);
          if (object instanceof Point) {
            Point point = (Point) object;
            coordinate = new Coordinate(point.getCoordinates().getLatitude(), point.getCoordinates().getLongitude());
          } else if (object instanceof MultiPoint) {
            MultiPoint point = (MultiPoint) object;
            coordinate = new Coordinate(point.getCoordinates().get(0).getLatitude(), point.getCoordinates().get(0).getLongitude());
          } else if (object instanceof Polygon) {
            Polygon point = (Polygon) object;
            coordinate = new Coordinate(point.getCoordinates().get(0).get(0).getLatitude(), point.getCoordinates().get(0).get(0).getLongitude());
          } else if (object instanceof MultiPolygon) {
            MultiPolygon point = (MultiPolygon) object;
            coordinate = new Coordinate(point.getCoordinates().get(0).get(0).get(0).getLatitude(), point.getCoordinates().get(0).get(0).get(0).getLongitude());
          } else {
            throw new IllegalArgumentException("type " + object + " not correctly handle in Fongo");
          }
        } catch (IOException e) {
          LOG.warn("don't known how to handle " + value);
        }
      } else if (dbObject.containsField("lng") && dbObject.containsField("lat")) {
        coordinate = new Coordinate(((Number) dbObject.get("lat")).doubleValue(), ((Number) dbObject.get("lng")).doubleValue());
      } else if (dbObject.containsField("x") && dbObject.containsField("y")) {
        coordinate = new Coordinate(((Number) dbObject.get("x")).doubleValue(), ((Number) dbObject.get("y")).doubleValue());
      } else if (dbObject.containsField("latitude") && dbObject.containsField("longitude")) {
        coordinate = new Coordinate(((Number) dbObject.get("latitude")).doubleValue(), ((Number) dbObject.get("longitude")).doubleValue());
      }
    } else if (value instanceof double[]) {
      double[] array = (double[]) value;
      if (array.length >= 2) {
        coordinate = new Coordinate(((Number) array[0]).doubleValue(), ((Number) array[1]).doubleValue());
      }
    }
    return coordinate;
  }

  public static Geometry toGeometry(Object object) {
    if (ExpressionParser.isDbObject(object)) {
      return toGeometry(ExpressionParser.toDbObject(object));
    }
    return createGeometryPoint(coordinate(object));
  }

  public static Geometry toGeometry(Coordinate coordinate) {
    return createGeometryPoint(coordinate);
  }

  public static Geometry toGeometry(DBObject dbObject) {
    if (dbObject.containsField("$box")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$box");
      return createBox(coordinates);
    } else if (dbObject.containsField("$center")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$center");
      return createCircle(coordinates, false);
    } else if (dbObject.containsField("$centerSphere")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$centerSphere");
      return createCircle(coordinates, true);
    } else if (dbObject.containsField("$polygon")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$polygon");
      return createPolygon(coordinates);
    } else if (dbObject.containsField("$geometry")) {
      // TODO : must check
      return toGeometry(ExpressionParser.toDbObject(dbObject.get("$geometry")));
    } else if (dbObject.containsField("type")) {
      try {
        GeoJsonObject geoJsonObject = new ObjectMapper().readValue(FongoJSON.serialize(dbObject), GeoJsonObject.class);
        if (geoJsonObject instanceof Point) {
          Point point = (Point) geoJsonObject;
          return createGeometryPoint(toCoordinate(point.getCoordinates()));
        } else if (geoJsonObject instanceof MultiPoint) {
          MultiPoint points = (MultiPoint) geoJsonObject;
          return toJtsMultiPoint(points.getCoordinates());
        } else if (geoJsonObject instanceof Polygon) {
          Polygon polygon = (Polygon) geoJsonObject;
          return toJtsPolygon(polygon.getCoordinates());
        } else if (geoJsonObject instanceof MultiPolygon) {
          MultiPolygon polygon = (MultiPolygon) geoJsonObject;
          return GEOMETRY_FACTORY.createMultiPolygon(toJtsPolygons(polygon.getCoordinates()));
        }
      } catch (IOException e) {
        LOG.warn("cannot handle " + FongoJSON.serialize(dbObject));
      }
    } else {
      Coordinate coordinate = coordinate(dbObject);
      if (coordinate != null) {
        return createGeometryPoint(coordinate);
      }
    }
    if (illegalForUnknownGeometry) {
      throw new IllegalArgumentException("can't handle " + FongoJSON.serialize(dbObject));
    }
    return null;
  }

  private static Geometry toJtsMultiPoint(List<LngLatAlt> lngLatAlts) {
    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    for (LngLatAlt lngLatAlt : lngLatAlts) {
      coordinates.add(toCoordinate(lngLatAlt));
    }
    return GEOMETRY_FACTORY.createMultiPoint(coordinates.toArray(new Coordinate[0]));
  }

  public static com.vividsolutions.jts.geom.Polygon toJtsPolygon(List<List<LngLatAlt>> lngLatAlts) {
    // it's a trick to ensure that the generated geometry is a closed one.
    if (lngLatAlts.size() > 1) {
      if (!lngLatAlts.get(lngLatAlts.size() - 1).equals(lngLatAlts.get(0))) {
        lngLatAlts = new ArrayList<List<LngLatAlt>>(lngLatAlts);
        lngLatAlts.add(lngLatAlts.get(0));
      }
    }
    return GEOMETRY_FACTORY.createPolygon(toCoordinates(lngLatAlts));
  }

  private static com.vividsolutions.jts.geom.Polygon[] toJtsPolygons(List<List<List<LngLatAlt>>> listPolygonsLngLatAlt) {
    List<com.vividsolutions.jts.geom.Polygon> polygons = new ArrayList<com.vividsolutions.jts.geom.Polygon>();
    for (List<List<LngLatAlt>> lngLatAlts : listPolygonsLngLatAlt) {
      polygons.add(toJtsPolygon(lngLatAlts));
    }
    return polygons.toArray(new com.vividsolutions.jts.geom.Polygon[0]);
  }

  private static Coordinate[] toCoordinates(List<List<LngLatAlt>> lngLatAlts) {
    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    for (List<LngLatAlt> lineStrings : lngLatAlts) {
      for (LngLatAlt lngLatAlt : lineStrings) {
        coordinates.add(toCoordinate(lngLatAlt));
      }
    }
    return coordinates.toArray(new Coordinate[0]);
  }

  public static Coordinate toCoordinate(LngLatAlt lngLatAlt) {
    return new Coordinate(lngLatAlt.getLatitude(), lngLatAlt.getLongitude(), lngLatAlt.getAltitude());
  }

  private static Geometry createBox(BasicDBList coordinates) {
    Coordinate[] t = parseCoordinates(coordinates);
    return GEOMETRY_FACTORY.toGeometry(new Envelope(t[0], t[1]));
  }

  private static Geometry createCircle(final BasicDBList coordinates, final boolean spherical) {
    Coordinate[] t = parseCoordinates(coordinates);

    double radius = ((Number) coordinates.get(1)).doubleValue();
    if (spherical) {
      radius *= EARTH_RADIUS;
    } else {
      radius *= METERS_PER_DEGREE;
    }

    return createCircle(t[0].x, t[0].y, radius);
  }

  public static Geometry createCircle(final double x, final double y, final double radius) {
    final int sides = 32;
    final Coordinate coords[] = new Coordinate[sides + 1];
    for (int i = 0; i < sides; i++) {
      final double angle = 360.0 * i / sides;
      coords[i] = destVincenty(x, y, angle, radius);
    }
    coords[sides] = coords[0];

    final LinearRing ring = GEOMETRY_FACTORY.createLinearRing(coords);
    return GEOMETRY_FACTORY.createPolygon(ring, null);
  }

  private static Coordinate destVincenty(final double longitude, final double latitude, final double angle,
                                         final double distanceInMeters) {
    // WGS-84 ellipsiod
    final double semiMajorAxis = 6378137;
    final double b = 6356752.3142;
    final double inverseFlattening = 1 / 298.257223563;
    final double alpha1 = Math.toRadians(angle);
    final double sinAlpha1 = Math.sin(alpha1);
    final double cosAlpha1 = Math.cos(alpha1);

    final double tanU1 = (1 - inverseFlattening) * Math.tan(Math.toRadians(latitude));
    final double cosU1 = 1 / Math.sqrt(1 + tanU1 * tanU1), sinU1 = tanU1 * cosU1;
    final double sigma1 = Math.atan2(tanU1, cosAlpha1);
    final double sinAlpha = cosU1 * sinAlpha1;
    final double cosSqAlpha = 1 - sinAlpha * sinAlpha;
    final double uSq = cosSqAlpha * (semiMajorAxis * semiMajorAxis - b * b) / (b * b);
    final double aa = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
    final double ab = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));

    double sigma = distanceInMeters / (b * aa);
    double sigmaP = 2 * Math.PI;
    double sinSigma = 0;
    double cosSigma = 0;
    double cos2SigmaM = 0;
    double deltaSigma = 0;
    while (Math.abs(sigma - sigmaP) > 1e-12) {
      cos2SigmaM = Math.cos(2 * sigma1 + sigma);
      sinSigma = Math.sin(sigma);
      cosSigma = Math.cos(sigma);
      deltaSigma = ab
          * sinSigma
          * (cos2SigmaM + ab
          / 4
          * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - ab / 6 * cos2SigmaM
          * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));
      sigmaP = sigma;
      sigma = distanceInMeters / (b * aa) + deltaSigma;
    }

    final double tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1;
    final double lat2 = Math.atan2(sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1,
        (1 - inverseFlattening) * Math.sqrt(sinAlpha * sinAlpha + tmp * tmp));
    final double lambda = Math.atan2(sinSigma * sinAlpha1, cosU1 * cosSigma - sinU1 * sinSigma * cosAlpha1);
    final double c = inverseFlattening / 16 * cosSqAlpha * (4 + inverseFlattening * (4 - 3 * cosSqAlpha));
    final double l = lambda - (1 - c) * inverseFlattening * sinAlpha
        * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));

    return new Coordinate(round(longitude + Math.toDegrees(l)), round(Math.toDegrees(lat2)));
  }

  private static double round(double dis) {
    double mul = 1000000D;
    return Math.round(dis * mul) / mul;
  }

  private static Geometry createPolygon(BasicDBList coordinates) {
    Coordinate[] t = parseCoordinates(coordinates);
    if (!t[0].equals(t[t.length - 1])) {
      Coordinate[] another = new Coordinate[t.length + 1];
      System.arraycopy(t, 0, another, 0, t.length);
      another[t.length] = t[0];
      t = another;
    }

    return GEOMETRY_FACTORY.createPolygon(t);
  }

  private static Coordinate[] parseCoordinates(BasicDBList coordinates) {
    Coordinate[] ret = new Coordinate[coordinates.size()];
    for (int i = 0, length = coordinates.size(); i < length; i++) {
      ret[i] = coordinate(coordinates.get(i));
    }

    return ret;
  }
}
