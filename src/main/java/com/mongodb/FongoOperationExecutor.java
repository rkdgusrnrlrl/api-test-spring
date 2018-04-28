/**
 * Copyright (C) 2017 Deveryware S.A. All Rights Reserved.
 */
package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.session.ClientSession;

/**
 *
 */
public class FongoOperationExecutor implements OperationExecutor {
  private final Fongo fongo;

  FongoOperationExecutor(Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public <T> T execute(ReadOperation<T> operation, ReadPreference readPreference) {
    return fongo.execute(operation, readPreference);
  }

  @Override
  public <T> T execute(WriteOperation<T> operation) {
    return fongo.execute(operation);
  }

  @Override
  public <T> T execute(ReadOperation<T> operation, ReadPreference readPreference, ClientSession session) {
    return fongo.execute(operation, readPreference, session);
  }

  @Override
  public <T> T execute(WriteOperation<T> operation, ClientSession session) {
    return fongo.execute(operation, session);
  }
}
