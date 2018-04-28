package com.mongodb;

import com.github.fakemongo.Fongo;
import com.github.fakemongo.FongoConnection;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.selector.ServerSelector;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.objenesis.ObjenesisHelper;
import org.objenesis.ObjenesisStd;

public class MockMongoClient extends MongoClient {

  private volatile BufferProvider bufferProvider;

  private Fongo fongo;
  private MongoOptions options;
  private ReadConcern readConcern;
  private MongoClientOptions clientOptions;

  public static MockMongoClient create(Fongo fongo) {
    // using objenesis here to prevent default constructor from spinning up background threads.
//    MockMongoClient client = new ObjenesisStd().getInstantiatorOf(MockMongoClient.class).newInstance();
    MockMongoClient client = ObjenesisHelper.newInstance(MockMongoClient.class);

    MongoClientOptions clientOptions = MongoClientOptions.builder().codecRegistry(fongo.getCodecRegistry()).build();
    client.clientOptions = clientOptions;
    client.options = new MongoOptions(clientOptions);
    client.fongo = fongo;
    client.setWriteConcern(clientOptions.getWriteConcern());
    client.setReadPreference(clientOptions.getReadPreference());
    client.readConcern = clientOptions.getReadConcern() == null ? ReadConcern.DEFAULT : clientOptions.getReadConcern();
    return client;
  }

  public MockMongoClient() throws UnknownHostException {
  }

  @Override
  public String toString() {
    return fongo.toString();
  }

  @Override
  public Collection<DB> getUsedDatabases() {
    return fongo.getUsedDatabases();
  }

  @Override
  public List<String> getDatabaseNames() {
    return fongo.getDatabaseNames();
  }

  @Override
  public ReplicaSetStatus getReplicaSetStatus() {
//    return new ReplicaSetStatus(getCluster());
    return null;
  }

  @Override
  public int getMaxBsonObjectSize() {
    return 16 * 1024 * 1024;
  }

  @Override
  public DB getDB(String dbname) {
    return this.fongo.getDB(dbname);
  }

  @Override
  public MongoDatabase getDatabase(final String databaseName) {
    return new FongoMongoDatabase(databaseName, this.fongo);
  }

  @Override
  public void dropDatabase(String dbName) {
    this.fongo.dropDatabase(dbName);
  }

  @Override
  public MongoOptions getMongoOptions() {
    return this.options;
  }

  @Override
  public MongoClientOptions getMongoClientOptions() {
    return clientOptions;
  }

  @Override
  public List<ServerAddress> getAllAddress() {
    return getServerAddressList();
  }

  @Override
  public ServerAddress getAddress() {
    return fongo.getServerAddress();
  }

  @Override
  public List<ServerAddress> getServerAddressList() {
    return Collections.singletonList(fongo.getServerAddress());
  }

  private ServerDescription getServerDescription() {
    return ServerDescription.builder().address(fongo.getServerAddress()).state(ServerConnectionState.CONNECTED).version(fongo.getServerVersion()).build();
  }

  @Override
  public Cluster getCluster() {
    return new Cluster() {
      @Override
      public ClusterSettings getSettings() {
        return ClusterSettings.builder().hosts(getServerAddressList())
            .requiredReplicaSetName(options.getRequiredReplicaSetName())
//            .serverSelectionTimeout(options.getServerSelectionTimeout(), MILLISECONDS)
//            .serverSelector(createServerSelector(options))
            .description(options.getDescription())
            .maxWaitQueueSize(10).build();
      }

      @Override
      public ClusterDescription getDescription() {
        return new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, Collections.singletonList(getServerDescription()));
      }

      @Override
      public ClusterDescription getCurrentDescription() {
        ClusterDescription description = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE,
            Collections.<ServerDescription>emptyList(), getSettings(),
//            new ServerFactoryImpl().getSettings());
            null);
        return description;
      }

      @Override
      public Server selectServer(ServerSelector serverSelector) {
        return new Server() {
          @Override
          public ServerDescription getDescription() {
            return new ObjenesisStd().getInstantiatorOf(ServerDescription.class).newInstance();
          }

          @Override
          public Connection getConnection() {
            return new FongoConnection(fongo);
          }

          @Override
          public void getConnectionAsync(SingleResultCallback<AsyncConnection> callback) {
            // TODO
          }

        };
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
    };
  }

  com.mongodb.OperationExecutor createOperationExecutor() {
    return new FongoOperationExecutor(fongo);
  }

  @Override
  public void close() {
  }

  @Override
  synchronized BufferProvider getBufferProvider() {
    if (bufferProvider == null) {
      bufferProvider = new PowerOfTwoBufferPool();
    }
    return bufferProvider;
  }

  @Override
  public ReadConcern getReadConcern() {
    return readConcern;
  }
}
