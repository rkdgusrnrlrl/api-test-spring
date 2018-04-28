package com.mongodb;

import java.util.List;
import static java.util.Collections.emptyList;

public class InsertManyWriteConcernException extends RuntimeException {

  private BulkWriteResult result;
  private List<FongoBulkWriteCombiner.WriteError> errors = emptyList();

  public InsertManyWriteConcernException(BulkWriteResult result, List<FongoBulkWriteCombiner.WriteError> errors) {
    this.result = result;
    this.errors = errors;
  }

  public BulkWriteResult getResult() {
    return result;
  }

  public List<FongoBulkWriteCombiner.WriteError> getErrors() {
    return errors;
  }

}
