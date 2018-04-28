package com.github.fakemongo.impl.aggregation;

import com.github.fakemongo.impl.ExpressionParser;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.annotations.ThreadSafe;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Match extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Match.class);

  public static final Match INSTANCE = new Match();

  private Match() {
  }

  /**
   * {@see http://docs.mongodb.org/manual/reference/aggregation/match/#pipe._S_match}
   */
  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    LOG.debug("computeResult() match : {}", object);

    List<DBObject> objects = coll.find(ExpressionParser.toDbObject(object.get(getKeyword()))).toArray();
    coll = dropAndInsert(coll, objects);
    LOG.debug("computeResult() match : {}, result : {}", object, objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$match";
  }

}
