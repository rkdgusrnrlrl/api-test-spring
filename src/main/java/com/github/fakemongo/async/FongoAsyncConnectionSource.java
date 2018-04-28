package com.github.fakemongo.async;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.session.SessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class FongoAsyncConnectionSource implements AsyncConnectionSource {
  private static final Logger LOG = LoggerFactory.getLogger(FongoAsyncConnectionSource.class);

  private final FongoAsync fongoAsync;

  public FongoAsyncConnectionSource(FongoAsync fongoAsync) {
    this.fongoAsync = fongoAsync;
  }

  @Override
  public ServerDescription getServerDescription() {
    return ServerDescription.builder().address(fongoAsync.getServerAddress()).state(ServerConnectionState.CONNECTED).version(fongoAsync.getServerVersion()).build();
  }

  @Override
  public SessionContext getSessionContext() {
    return NoOpSessionContext.INSTANCE;
  }

  @Override
  public void getConnection(SingleResultCallback<AsyncConnection> callback) {
    callback.onResult(new FongoAsyncConnection(this.fongoAsync), null);
  }

  @Override
  public AsyncConnectionSource retain() {
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
