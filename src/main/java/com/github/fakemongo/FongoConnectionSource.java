package com.github.fakemongo;

import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.session.SessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class FongoConnectionSource implements ConnectionSource {
  private static final Logger LOG = LoggerFactory.getLogger(FongoConnectionSource.class);

  private final Fongo fongo;

  FongoConnectionSource(Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public ServerDescription getServerDescription() {
    return ServerDescription.builder().address(fongo.getServerAddress()).state(ServerConnectionState.CONNECTED).version(fongo.getServerVersion()).build();
  }

  @Override
  public SessionContext getSessionContext() {
    return NoOpSessionContext.INSTANCE;
  }

  @Override
  public Connection getConnection() {
    return new FongoConnection(this.fongo);
  }

  @Override
  public ConnectionSource retain() {
    return this;
  }

  @Override
  public int getCount() {
    return 0;
  }

  @Override
  public void release() {
  }
}
