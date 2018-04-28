package com.github.fakemongo.impl.aggregation;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.annotations.ThreadSafe;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

/**
 */
@ThreadSafe
public class Sample extends PipelineKeyword {
  public static final Sample INSTANCE = new Sample();

  private static final Random rnd = new Random();

  // Based on Floyd's random sample algorithm, taken from here: http://stackoverflow.com/a/3724708/736741
  private static Set<Integer> randomSample(int max, int n) {
    HashSet<Integer> res = new HashSet<Integer>(n);
    int count = max + 1;
    for (int i = count - n; i < count; i++) {
      Integer item = rnd.nextInt(i + 1);
      if (res.contains(item))
        res.add(i);
      else
        res.add(item);
    }
    return res;
  }

  /**
   */
  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    DBObject dbObject = (DBObject) object.get(getKeyword());
    int size = ((Number) dbObject.get("size")).intValue();

    List<DBObject> objects = new ArrayList<DBObject>(size);
    int count = (int) coll.count();
    if (count <= size) {  // no need to sample, collection has less elements than we want to sample
      return coll;
    }

    if (count != 0) {
      Set<Integer> samples = randomSample(count - 1, size);
      List<DBObject> collAsArray = coll.find().toArray();
      for (Integer sample : samples) {
        objects.add(collAsArray.get(sample));
      }
    }

    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$sample";
  }
}
