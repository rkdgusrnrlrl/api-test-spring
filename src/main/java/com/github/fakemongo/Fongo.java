package com.github.fakemongo;

import com.mongodb.DB;
import com.mongodb.FongoBulkWriteCombiner;
import com.mongodb.FongoDB;
import com.mongodb.MockMongoClient;
import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.session.ClientSession;
import com.mongodb.session.SessionContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Faked out version of com.mongodb.Mongo
 * <p>
 * This class doesn't implement Mongo, but does provide the same basic interface
 * </p>
 * Usage:
 * <pre>
 * {@code
 * Fongo fongo = new Fongo("test server");
 * com.mongodb.DB db = fongo.getDB("mydb");
 * // if you need an instance of com.mongodb.Mongo
 * com.mongodb.MongoClient mongo = fongo.getMongo();
 * }
 * </pre>
 *
 * @author jon
 * @author twillouer
 */
public class Fongo implements /* TODO REMOVE 3.6 */ OperationExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Fongo.class);

  public static final ServerVersion V3_2_SERVER_VERSION = new ServerVersion(3, 2);
  public static final ServerVersion V3_3_SERVER_VERSION = new ServerVersion(3, 3);
  public static final ServerVersion V3_6_SERVER_VERSION = new ServerVersion(3, 6);
  public static final ServerVersion V3_SERVER_VERSION = new ServerVersion(3, 0);
  public static final ServerVersion OLD_SERVER_VERSION = new ServerVersion(0, 0);
  public static final ServerVersion DEFAULT_SERVER_VERSION = V3_6_SERVER_VERSION;

  private final Map<String, FongoDB> dbMap = new ConcurrentHashMap<String, FongoDB>();
  private final ServerAddress serverAddress;
  private final MongoClient mongo;
  private final String name;
  private final ServerVersion serverVersion;
  private final CodecRegistry codecRegistry;

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(final String name) {
    this(name, DEFAULT_SERVER_VERSION);
  }

  /**
   * @param name          Used only for a nice toString in case you have multiple instances
   * @param serverVersion version of the server to use for fongo.
   */
  public Fongo(final String name, final ServerVersion serverVersion) {
    this(name, serverVersion, MongoClient.getDefaultCodecRegistry());
  }

  /**
   * @param name          Used only for a nice toString in case you have multiple instances
   * @param serverVersion version of the server to use for fongo.
   * @param codecRegistry the codec registry used by fongo.
   */
  public Fongo(final String name, final ServerVersion serverVersion, final CodecRegistry codecRegistry) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultHost(), ServerAddress.defaultPort()));
    this.serverVersion = serverVersion;
    this.codecRegistry = codecRegistry;
    this.mongo = createMongo();
  }

  /**
   * equivalent to getDB in driver
   * multiple calls to this method return the same DB instance
   *
   * @param dbname name of the db.
   * @return the DB associated to this name.
   */
  public FongoDB getDB(String dbname) {
    synchronized (dbMap) {
      FongoDB fongoDb = dbMap.get(dbname);
      if (fongoDb == null) {
        fongoDb = new FongoDB(this, dbname);
        dbMap.put(dbname, fongoDb);
      }
      return fongoDb;
    }
  }

  public synchronized MongoDatabase getDatabase(final String databaseName) {
    return mongo.getDatabase(databaseName);
  }

  /**
   * Get databases that have been used
   *
   * @return database names.
   */
  public Collection<DB> getUsedDatabases() {
    return new ArrayList<DB>(dbMap.values());
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
    FongoDB db = dbMap.remove(dbName);
    if (db != null) {
      db.dropDatabase();
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

  public WriteConcern getWriteConcern() {
    return mongo.getWriteConcern();
  }

  public ReadConcern getReadConcern() {
    return mongo.getReadConcern();
  }

  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  private MongoClient createMongo() {
    return MockMongoClient.create(this);
  }

  public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ClientSession clientSession) {
    return operation.execute(new ReadBinding() {
      @Override
      public ReadPreference getReadPreference() {
        return readPreference;
      }

      @Override
      public ConnectionSource getReadConnectionSource() {
        return new FongoConnectionSource(Fongo.this);
      }

      @Override
      public SessionContext getSessionContext() {
        return clientSession == null ? NoOpSessionContext.INSTANCE : new ClientSessionContext(clientSession);
      }

      @Override
      public ReadBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    });
  }

  public <T> T execute(final WriteOperation<T> operation, final ClientSession clientSession) {
    if (operation instanceof MixedBulkWriteOperation) {
      MixedBulkWriteOperation mixedBulkWriteOperation = (MixedBulkWriteOperation) operation;
      FongoBulkWriteCombiner fongoBulkWriteCombiner = new FongoBulkWriteCombiner(mixedBulkWriteOperation.getWriteConcern());
      int i = 0;
      for (WriteRequest writeRequest : mixedBulkWriteOperation.getWriteRequests()) {
        if (writeRequest instanceof InsertRequest) {
          InsertRequest insertRequest = (InsertRequest) writeRequest;
          final WriteConcernResult update = new FongoConnection(this).insert(mixedBulkWriteOperation.getNamespace(), mixedBulkWriteOperation.isOrdered(), insertRequest);
          fongoBulkWriteCombiner.addInsertResult(update);
        } else if (writeRequest instanceof UpdateRequest) {
          UpdateRequest updateRequest = (UpdateRequest) writeRequest;
          final WriteConcernResult update = new FongoConnection(this).update(mixedBulkWriteOperation.getNamespace(), mixedBulkWriteOperation.isOrdered(), updateRequest);
          fongoBulkWriteCombiner.addUpdateResult(i, update);
        } else if (writeRequest instanceof DeleteRequest) {
          DeleteRequest deleteRequest = (DeleteRequest) writeRequest;
          final WriteConcernResult update = new FongoConnection(this).delete(mixedBulkWriteOperation.getNamespace(), mixedBulkWriteOperation.isOrdered(), deleteRequest);
          fongoBulkWriteCombiner.addRemoveResult(update);
        } else {
          throw new FongoException("Fongo doesn't implement " + writeRequest.getClass());
        }
        i++;
      }
      return (T) fongoBulkWriteCombiner.toBulkWriteResult();
    } else if (operation instanceof UpdateOperation) {
      final UpdateOperation updateOperation = (UpdateOperation) operation;
      final FongoBulkWriteCombiner fongoBulkWriteCombiner = new FongoBulkWriteCombiner(updateOperation.getWriteConcern());
      int i = 0;
      for (UpdateRequest updateRequest : updateOperation.getUpdateRequests()) {
        final WriteConcernResult update = new FongoConnection(this).update(updateOperation.getNamespace(), updateOperation.isOrdered(), updateRequest);
        fongoBulkWriteCombiner.addUpdateResult(i, update);
        i++;
      }
      return (T) fongoBulkWriteCombiner.toWriteConcernResult();
    } else if (operation instanceof InsertOperation) {
      final InsertOperation insertOperation = (InsertOperation) operation;
      final FongoBulkWriteCombiner fongoBulkWriteCombiner = new FongoBulkWriteCombiner(insertOperation.getWriteConcern());
      int i = 0;
      for (InsertRequest insertRequest : insertOperation.getInsertRequests()) {
        final WriteConcernResult update = new FongoConnection(this).insert(insertOperation.getNamespace(), insertOperation.isOrdered(), insertRequest);
        fongoBulkWriteCombiner.addInsertResult(update);
        i++;
      }
      return (T) fongoBulkWriteCombiner.toWriteConcernResult();
    } else if (operation instanceof DeleteOperation) {
      final DeleteOperation deleteOperation = (DeleteOperation) operation;
      final FongoBulkWriteCombiner fongoBulkWriteCombiner = new FongoBulkWriteCombiner(deleteOperation.getWriteConcern());
      int i = 0;
      for (DeleteRequest deleteRequest : deleteOperation.getDeleteRequests()) {
        final WriteConcernResult update = new FongoConnection(this).delete(deleteOperation.getNamespace(), deleteOperation.isOrdered(), deleteRequest);
        fongoBulkWriteCombiner.addRemoveResult(update);
        i++;
      }
      return (T) fongoBulkWriteCombiner.toWriteConcernResult();
    }

    return operation.execute(new WriteBinding() {
      @Override
      public ConnectionSource getWriteConnectionSource() {
        return new FongoConnectionSource(Fongo.this);
      }

      @Override
      public SessionContext getSessionContext() {
        return clientSession == null ? NoOpSessionContext.INSTANCE : new ClientSessionContext(clientSession);
      }

      @Override
      public WriteBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    });
  }


  @Override
  public String toString() {
    return "Fongo (" + this.name + ")";
  }

  public ServerVersion getServerVersion() {
    return serverVersion;
  }

  // TODO REMOVE 3.6
  @Override
  public <T> T execute(ReadOperation<T> operation, ReadPreference readPreference) {
    return execute(operation, readPreference, null);
  }

  // TODO REMOVE 3.6
  @Override
  public <T> T execute(WriteOperation<T> operation) {
    return execute(operation, null);
  }
}
