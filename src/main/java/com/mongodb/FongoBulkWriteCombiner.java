package com.mongodb;

import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class FongoBulkWriteCombiner {

  private final WriteConcern writeConcern;

  private int insertedCount = 0;
  private int matchedCount = 0;
  private int removedCount = 0;
  private int modifiedCount = 0;

  private final Set<com.mongodb.BulkWriteUpsert> upserts = new TreeSet<com.mongodb.BulkWriteUpsert>(new Comparator<com.mongodb.BulkWriteUpsert>() {
    @Override
    public int compare(final com.mongodb.BulkWriteUpsert o1, final com.mongodb.BulkWriteUpsert o2) {
      return (o1.getIndex() < o2.getIndex()) ? -1 : ((o1.getIndex() == o2.getIndex()) ? 0 : 1);
    }
  });
  private final TreeSet<WriteError> errors = new TreeSet<WriteError>(new Comparator<WriteError>() {
    @Override
    public int compare(final WriteError o1, final WriteError o2) {
      return (o1.getIndex() < o2.getIndex()) ? -1 : ((o1.getIndex() == o2.getIndex()) ? 0 : 1);
    }
  });

  public FongoBulkWriteCombiner(WriteConcern writeConcern) {
    this.writeConcern = writeConcern;
  }

  public void addReplaceResult(int idx, WriteResult wr) {
    // Same logic applies
    addUpdateResult(idx, wr);
  }

  public void addUpdateResult(int idx, WriteResult wr) {
    if (wr.isUpdateOfExisting()) {
      matchedCount += wr.getN();
      modifiedCount += wr.getN();
    } else {
      if (wr.getUpsertedId() != null) {
        upserts.add(new BulkWriteUpsert(idx, wr.getUpsertedId()));
      }
    }
  }

  // TODO
  public void addUpdateResult(int idx, WriteConcernResult wr) {
    if (wr.isUpdateOfExisting()) {
      matchedCount += wr.getCount();
      modifiedCount += wr.getCount();
    } else {
      if (wr.getUpsertedId() != null) {
        upserts.add(new BulkWriteUpsert(idx, wr.getUpsertedId()));
      }
    }
  }


  public void addRemoveResult(WriteResult wr) {
    matchedCount += wr.getN();
    removedCount += wr.getN();
  }

  public void addRemoveResult(WriteConcernResult wr) {
    matchedCount += 0; //wr.getCount();
    removedCount += wr.getCount();
  }

  public void addInsertResult(WriteResult wr) {
    insertedCount += wr.getN();
  }

  public void addInsertResult(WriteConcernResult wr) {
    insertedCount += wr.getCount();
  }

  public void addInsertError(int idx, WriteConcernException exception) {
    errors.add(new WriteError(idx, exception));
  }

  public BulkWriteResult getBulkWriteResult(WriteConcern writeConcern) {
    if (!writeConcern.isAcknowledged()) {
      return new UnacknowledgedBulkWriteResult();
    }
    return new AcknowledgedBulkWriteResult(insertedCount, matchedCount, removedCount, modifiedCount, new ArrayList<BulkWriteUpsert>(upserts));
  }

  public void throwOnError() {
    if (!errors.isEmpty()) {
      BulkWriteResult bulkWriteResult = getBulkWriteResult(writeConcern);
      throw new InsertManyWriteConcernException(bulkWriteResult, unmodifiableList(new ArrayList<WriteError>(errors)));
    }
  }

  public com.mongodb.bulk.BulkWriteResult toBulkWriteResult() {
    if (this.writeConcern.isAcknowledged()) {
      final ArrayList<com.mongodb.bulk.BulkWriteUpsert> bulkWriteUpserts = new ArrayList<com.mongodb.bulk.BulkWriteUpsert>();
      for (BulkWriteUpsert upsert : this.upserts) {
        bulkWriteUpserts.add(new com.mongodb.bulk.BulkWriteUpsert(upsert.getIndex(), (BsonValue) upsert.getId()));
      }
      return com.mongodb.bulk.BulkWriteResult.acknowledged(insertedCount, matchedCount, removedCount, modifiedCount, bulkWriteUpserts);
    } else {
      return com.mongodb.bulk.BulkWriteResult.unacknowledged();
    }
  }

  public com.mongodb.WriteConcernResult toWriteConcernResult() {
    if (this.writeConcern.isAcknowledged()) {
      return WriteConcernResult.acknowledged(insertedCount, matchedCount > 0, upserts.isEmpty() ? null : (BsonValue) upserts.iterator().next().getId());
    } else {
      return WriteConcernResult.unacknowledged();
    }
  }

  public static class WriteError {

    private final int index;
    private final int code;
    private final String message;
    private final BsonDocument details;
    private final WriteConcernException exception;

    public WriteError(int index, WriteConcernException exception) {
      this.index = index;
      this.exception = exception;
      this.code = exception.getErrorCode();
      this.message = exception.getErrorMessage();
      this.details = exception.getResponse();
    }

    public int getIndex() {
      return index;
    }

    public int getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }

    public BsonDocument getDetails() {
      return details;
    }

    public WriteConcernException getException() {
      return exception;
    }

  }

}
