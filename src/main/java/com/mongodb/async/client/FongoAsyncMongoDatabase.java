/**
 * Copyright (C) 2016 Deveryware S.A. All Rights Reserved.
 */
package com.mongodb.async.client;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.operation.AsyncOperationExecutor;
import org.bson.codecs.configuration.CodecRegistry;

/**
 *
 */
public class FongoAsyncMongoDatabase extends MongoDatabaseImpl {
  public FongoAsyncMongoDatabase(String name, CodecRegistry codecRegistry, ReadPreference readPreference, WriteConcern writeConcern, boolean retryWrites, ReadConcern readConcern, AsyncOperationExecutor executor) {
    super(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
  }

}
