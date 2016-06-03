package com.talena.agents.couchbase.cstore;

import com.talena.agents.couchbase.core.CouchbaseLongRecord;
import com.talena.agents.couchbase.cstore.Bucket;
import com.talena.agents.couchbase.cstore.CStore;
import com.talena.agents.couchbase.cstore.MutationsMapper;

import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.$colon$colon;
import scala.reflect.ClassTag;

public class RestoreExample extends MutationsMapper<Void> {
  public void restore() {
    List buckets = getBuckets(new Bucket("sales", 1024, 1024));
    ClassTag<Void> tag = scala.reflect.ClassTag$.MODULE$.apply(Void.class);
    CStore.mapMutations(buckets, "/Production/SalesBackup/20161229", this, tag);
  }

  @Override
  public void setup() {}

  @Override
  public Void map(CouchbaseLongRecord rec) {
    System.out.println("Rec: " + rec.toString());
    return null;
  }

  @Override
  public void teardown() {}

  private List<Bucket> getBuckets(Bucket ... bucketsIn) {
    List<Bucket> bucketsOut = List$.MODULE$.empty();
    for(int i = bucketsIn.length; i > 0; i--) {
      bucketsOut = new $colon$colon(bucketsIn[i - 1], bucketsOut);
    }
    return bucketsOut;
  }
}
