package com.mongodb.operation;

import java.util.List;

/**
 *
 */
public final class FongoBsonArrayWrapper {

  private FongoBsonArrayWrapper() {
  }

  public static <T> BsonArrayWrapper<T> bsonArrayWrapper(List<T> array) {
    return new BsonArrayWrapper<T>(array);
  }
}
