package com.github.fakemongo.junit;

import static com.github.fakemongo.Fongo.DEFAULT_SERVER_VERSION;
import com.github.fakemongo.async.FongoAsync;
import static com.github.fakemongo.junit.FongoRule.randomName;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.connection.ServerVersion;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.rules.ExternalResource;

/**
 * Create a Junit Rule to use with annotation
 * <p>
 * &#64;Rule
 * public FongoAsyncRule rule = new FongoAsyncRule().
 * </p>
 * <p>
 * Note than you can switch to a real mongodb on your localhost (for now).
 * </p>
 * <p><b>
 * WARNING : database is dropped after the test !!
 * </b></P>
 */
public class FongoAsyncRule extends ExternalResource {

  /**
   * Will be true if we use the real MongoDB to test things against real world.
   */
  private final boolean realMongo;
  private final String dbName;
  private final FongoAsync fongo;
  private MongoClient mongo;
  private MongoDatabase mongoDatabase;

  /**
   * Setup a rule with a real MongoDB.
   *
   * @param dbName            the dbName to use.
   * @param serverVersion     version of the server to use for fongo.
   * @param realMongo         set to true if you want to use a real mongoDB.
   * @param mongoClientIfReal real client to use if realMongo si true.
   */
  public FongoAsyncRule(final String dbName, final ServerVersion serverVersion, final boolean realMongo, final MongoClient mongoClientIfReal) {
    this.dbName = dbName;
    this.realMongo = realMongo || "true".equals(System.getProperty("fongo.force.realMongo"));
    this.fongo = realMongo ? null : newFongo(serverVersion);
    this.mongo = mongoClientIfReal;
  }

  public FongoAsyncRule() {
    this(DEFAULT_SERVER_VERSION);
  }

  public FongoAsyncRule(final ServerVersion serverVersion) {
    this(randomName(), serverVersion, false, null);
  }

  public FongoAsyncRule(boolean realMongo) {
    this(realMongo, DEFAULT_SERVER_VERSION);
  }

  public FongoAsyncRule(boolean realMongo, ServerVersion serverVersion) {
    this(randomName(), serverVersion, realMongo, null);
  }

  public FongoAsyncRule(boolean realMongo, MongoClient mongoClientIfReal) {
    this(randomName(), DEFAULT_SERVER_VERSION, realMongo, mongoClientIfReal);
  }

  public FongoAsyncRule(String dbName, boolean realMongo) {
    this(dbName, DEFAULT_SERVER_VERSION, realMongo, null);
  }

  public FongoAsyncRule(String dbName) {
    this(dbName, DEFAULT_SERVER_VERSION, false, null);
  }

  public boolean isRealMongo() {
    return this.realMongo;
  }

  @Override
  protected void before() throws UnknownHostException {
    if (realMongo) {
      if (mongo == null) {
        mongo = MongoClients.create();
      }
    } else {
      mongo = this.fongo.getMongo();
    }
    mongoDatabase = mongo.getDatabase(dbName);
  }

  @Override
  protected void after() {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    mongoDatabase.drop(new SingleResultCallback<Void>() {
      @Override
      public void onResult(Void result, Throwable t) {
        countDownLatch.countDown();
      }
    });
    try {
      countDownLatch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public MongoCollection<Document> newMongoCollection() {
    return newMongoCollection(randomName());
  }

  public MongoCollection<Document> newMongoCollection(final String collectionName) {
    return mongoDatabase.getCollection(collectionName);
  }

  public <T> MongoCollection<T> newMongoCollection(final Class<T> documentClass) {
    return newMongoCollection(randomName(), documentClass);
  }

  public <T> MongoCollection<T> newMongoCollection(final String collectionName, final Class<T> documentClass) {
    return mongoDatabase.getCollection(collectionName, documentClass);
  }

  protected FongoAsync newFongo(ServerVersion serverVersion) {
    return new FongoAsync("test", serverVersion);
  }

  public FongoAsync getFongo() {
    return this.fongo;
  }

  public MongoDatabase getDatabase(String name) {
    return this.mongo.getDatabase(name);
  }

  public MongoDatabase getDatabase() {
    return this.mongoDatabase;
  }

  public MongoClient getMongoClient() {
    return this.mongo;
  }

}
