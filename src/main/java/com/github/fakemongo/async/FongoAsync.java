package com.github.fakemongo.async;

import com.github.fakemongo.AwaitResultSingleResultCallback;
import com.github.fakemongo.Fongo;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FongoAsyncMongoDatabase;
import com.mongodb.async.client.MockAsyncMongoClient;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.session.ClientSession;
import com.mongodb.session.SessionContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Faked out version of com.mongodb.client.async.Mongo
 * <p>
 * This class doesn't implement Mongo, but does provide the same basic interface
 * </p>
 * Usage:
 * <pre>
 * {@code
 * FongoAsync fongo = new FongoAsync("test server");
 * // if you need an instance of com.mongodb.async.MongoClient
 * com.mongodb.async.MongoClient mongo = fongo.getMongoClient();
 * }
 * </pre>
 *
 * @author twillouer
 */
public class FongoAsync implements AsyncOperationExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FongoAsync.class);

  public static final ServerVersion DEFAULT_SERVER_VERSION = new ServerVersion(3, 6);

  private final Fongo fongo;
  private final Map<String, FongoAsyncMongoDatabase> dbMap = new ConcurrentHashMap<String, FongoAsyncMongoDatabase>();
  private final ServerAddress serverAddress;
  private final MongoClient mongo;
  private final String name;

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public FongoAsync(final String name) {
    this(name, DEFAULT_SERVER_VERSION);
  }

  /**
   * @param name          Used only for a nice toString in case you have multiple instances
   * @param serverVersion version of the server to use for fongo.
   */
  public FongoAsync(final String name, final ServerVersion serverVersion) {
    this(name, serverVersion, MongoClients.getDefaultCodecRegistry());
  }

  /**
   * @param name          Used only for a nice toString in case you have multiple instances
   * @param serverVersion version of the server to use for fongo.
   * @param codecRegistry the codec registry used by fongo.
   */
  public FongoAsync(final String name, final ServerVersion serverVersion, final CodecRegistry codecRegistry) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultHost(), ServerAddress.defaultPort()));
    this.fongo = new Fongo(name, serverVersion, codecRegistry);
    this.mongo = createMongo();
  }

  /**
   * equivalent to getDatabase in driver
   * multiple calls to this method return the same MongoDatabase instance
   *
   * @param databaseName name of the db.
   * @return the MongoDatabase associated to this name.
   */
  public synchronized FongoAsyncMongoDatabase getDatabase(final String databaseName) {
    synchronized (dbMap) {
      FongoAsyncMongoDatabase fongoAsyncMongoDatabase = dbMap.get(databaseName);
      if (fongoAsyncMongoDatabase == null) {
        fongoAsyncMongoDatabase = new FongoAsyncMongoDatabase(databaseName, mongo.getSettings().getCodecRegistry(), mongo.getSettings().getReadPreference(), mongo.getSettings().getWriteConcern(),
            false, mongo.getSettings().getReadConcern(), this);
        dbMap.put(databaseName, fongoAsyncMongoDatabase);
      }
      return fongoAsyncMongoDatabase;
    }
  }

  /**
   * Get database names that have been used
   *
   * @return database names.
   */
  public List<String> getDatabaseNames() {
    return new ArrayList<String>(dbMap.keySet());
  }

  /**
   * Drop db and all data from memory
   *
   * @param dbName name of the database.
   */
  public void dropDatabase(String dbName) {
    FongoAsyncMongoDatabase db = dbMap.remove(dbName);
    if (db != null) {
      db.drop(new AwaitResultSingleResultCallback<Void>());
    }
  }

  /**
   * This will always be localhost:27017
   *
   * @return the server address.
   */
  public ServerAddress getServerAddress() {
    return serverAddress;
  }

  /**
   * A mocked out instance of com.mongodb.Mongo
   * All methods calls are intercepted and execute associated Fongo method
   *
   * @return the mongo client
   */
  public MongoClient getMongo() {
    return this.mongo;
  }

  private MongoClient createMongo() {
    return MockAsyncMongoClient.create(this);
  }


  @Override
  public String toString() {
    return "FongoAsync (" + this.name + ")";
  }

  /**
   * Execute the read operation with the given read preference.
   *
   * @param operation      the read operation.
   * @param readPreference the read preference.
   * @param callback       the callback to be called when the operation has been executed
   */
  @Override
  public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, SingleResultCallback<T> callback) {
    execute(operation, readPreference, null, callback);

  }

  @Override
  public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, ClientSession session, SingleResultCallback<T> callback) {
    operation.executeAsync(new AsyncReadBinding() {
      @Override
      public ReadPreference getReadPreference() {
        return readPreference;
      }

      @Override
      public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
      }

      @Override
      public void getReadConnectionSource(SingleResultCallback<AsyncConnectionSource> callback) {
        LOG.info("getReadConnectionSource() operation:" + operation.getClass());
        callback.onResult(new FongoAsyncConnectionSource(FongoAsync.this), null);
      }

      @Override
      public AsyncReadBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    }, callback);
  }

  /**
   * Execute the write operation.
   *
   * @param operation the write operation.
   * @param callback  the callback to be called when the operation has been executed
   */
  @Override
  public <T> void execute(final AsyncWriteOperation<T> operation, SingleResultCallback<T> callback) {
    execute(operation, null, callback);
  }

  @Override
  public <T> void execute(final AsyncWriteOperation<T> operation, ClientSession session, SingleResultCallback<T> callback) {
    operation.executeAsync(new AsyncWriteBinding() {
      @Override
      public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        LOG.info("getWriteConnectionSource() operation:" + operation.getClass());
        callback.onResult(new FongoAsyncConnectionSource(FongoAsync.this), null);
      }

      @Override
      public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
      }

      @Override
      public AsyncWriteBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    }, callback);
  }

  public ServerVersion getServerVersion() {
    return fongo.getServerVersion();
  }

  public Fongo getFongo() {
    return fongo;
  }
}
