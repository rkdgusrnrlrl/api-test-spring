package com.mongodb.async.client;

import com.github.fakemongo.async.FongoAsync;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.selector.ServerSelector;
import java.util.Collections;
import org.bson.Document;

public class MockAsyncMongoClient extends MongoClientImpl {

  private final FongoAsync fongoAsync;

  public static MockAsyncMongoClient create(final FongoAsync fongoAsync) {
    // using objenesis here to prevent default constructor from spinning up background threads.
//    MockAsyncMongoClient client = new ObjenesisStd().getInstantiatorOf(MockAsyncMongoClient.class).newInstance();
    MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(fongoAsync.getFongo().getCodecRegistry()).build();
    MockAsyncMongoClient client = new MockAsyncMongoClient(fongoAsync, settings, new Cluster() {
      @Override
      public ClusterSettings getSettings() {
        return ClusterSettings.builder().build();
      }

      @Override
      public ClusterDescription getDescription() {
        return null;
      }

      @Override
      public ClusterDescription getCurrentDescription() {
        return new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE,
            Collections.<ServerDescription>emptyList(), getSettings(),
            null);
      }

      @Override
      public Server selectServer(ServerSelector serverSelector) {
        return null;
      }

      @Override
      public void selectServerAsync(ServerSelector serverSelector, SingleResultCallback<Server> callback) {

      }

      @Override
      public void close() {

      }

      @Override
      public boolean isClosed() {
        return false;
      }
    }, fongoAsync);
    return client;
  }

  public MockAsyncMongoClient(final FongoAsync fongoAsync, final MongoClientSettings settings, final Cluster cluster, final AsyncOperationExecutor executor) {
    super(settings, cluster, executor);
    this.fongoAsync = fongoAsync;
  }

  @Override
  public String toString() {
    return fongoAsync.toString();
  }


  /**
   * Gets the database with the given name.
   *
   * @param name the name of the database
   * @return the database
   */
  @Override
  public MongoDatabase getDatabase(String name) {
    return fongoAsync.getDatabase(name);
  }

  @Override
  public void close() {
  }

  /**
   * Gets the list of databases
   *
   * @return the list databases iterable interface
   */
  @Override
  public ListDatabasesIterable<Document> listDatabases() {
    return null;
  }

  /**
   * Gets the list of databases
   *
   * @param tResultClass the class to cast the database documents to
   * @return the list databases iterable interface
   */
  @Override
  public <TResult> ListDatabasesIterable<TResult> listDatabases(Class<TResult> tResultClass) {
    return null;
  }
}
