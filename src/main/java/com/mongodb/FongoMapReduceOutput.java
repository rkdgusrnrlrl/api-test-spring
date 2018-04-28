package com.mongodb;

import com.mongodb.operation.MapReduceBatchCursor;
import com.mongodb.operation.MapReduceStatistics;
import static java.util.Collections.singleton;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class FongoMapReduceOutput extends MapReduceOutput {

  public FongoMapReduceOutput(final DBObject command, final DBCollection collection, final MapReduceStatistics mapReduceStatistics) {
    super(command, collection.find(), mapReduceStatistics, collection);

  }

  public FongoMapReduceOutput(DBObject command, final List<DBObject> objects) {
    // TODO : verify minimal data.
    super(command, new MapReduceBatchCursor<DBObject>() {

      final Iterator<List<DBObject>> iterator = singleton(objects).iterator();

      @Override
      public MapReduceStatistics getStatistics() {
        return null;
      }

      @Override
      public void close() {
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public List<DBObject> next() {
        return iterator.next();
      }

      @Override
      public void setBatchSize(int batchSize) {

      }

      @Override
      public int getBatchSize() {
        return 0;
      }

      @Override
      public List<DBObject> tryNext() {
        return next();
      }

      @Override
      public ServerCursor getServerCursor() {
        return null;
      }

      @Override
      public ServerAddress getServerAddress() {
        return null;
      }

      @Override
      public void remove() {
        throw new IllegalStateException("cannot remove");
      }
    });
  }
}
