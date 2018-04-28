package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;

/**
 *
 */
public class FongoMongoDatabase extends MongoDatabaseImpl {

  private final Fongo fongo;

  public FongoMongoDatabase(final String databaseName, final Fongo fongo) {
    this(databaseName, fongo, fongo.getCodecRegistry(), ReadPreference.primary(), WriteConcern.ACKNOWLEDGED, ReadConcern.DEFAULT);
  }

  private FongoMongoDatabase(final String databaseName, final Fongo fongo, final CodecRegistry codecRegistry, final ReadPreference readPreference, final WriteConcern writeConcern, final ReadConcern readConcern) {
    super(databaseName, codecRegistry, readPreference, writeConcern, false, readConcern, new FongoOperationExecutor(fongo));
    this.fongo = fongo;
  }

  @Override
  public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
    return new FongoMongoDatabase(super.getName(), this.fongo, codecRegistry, super.getReadPreference(), super.getWriteConcern(), super.getReadConcern());
  }

  @Override
  public MongoDatabase withReadPreference(ReadPreference readPreference) {
    return new FongoMongoDatabase(super.getName(), this.fongo, super.getCodecRegistry(), readPreference, super.getWriteConcern(), super.getReadConcern());
  }

  @Override
  public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
    return new FongoMongoDatabase(super.getName(), this.fongo, super.getCodecRegistry(), super.getReadPreference(), writeConcern, super.getReadConcern());
  }

  @Override
  public MongoDatabase withReadConcern(final ReadConcern readConcern) {
    return new FongoMongoDatabase(super.getName(), this.fongo, super.getCodecRegistry(), super.getReadPreference(), super.getWriteConcern(), readConcern);
  }

  @Override
  public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
    return new FongoMongoCollection<TDocument>(this.fongo, new MongoNamespace(super.getName(), collectionName), documentClass, super.getCodecRegistry(), super.getReadPreference(),
        super.getWriteConcern(), super.getReadConcern());

  }
}
