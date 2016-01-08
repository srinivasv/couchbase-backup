package com.talena.agents.couchbase.mstore;

import com.talena.agents.couchbase.core.CouchbaseLongRecord;
import com.talena.agents.couchbase.mstore.BucketProps;
import com.talena.agents.couchbase.mstore.MStore;
import com.talena.agents.couchbase.mstore.MutationsMapper;

import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.$colon$colon;
import scala.reflect.ClassTag;

public class RestoreExample extends MutationsMapper<Void> {
  public void restore() {
  List bProps = getBucketProps(new BucketProps("sales", 1024));
  ClassTag<Void> tag = scala.reflect.ClassTag$.MODULE$.apply(Void.class);
  MStore.mapMutations("Production", "SalesBackup", bProps, "2016-12-29",
    this, tag);
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

  private List<BucketProps> getBucketProps(BucketProps ... props) {
    List<BucketProps> bProps = List$.MODULE$.empty();
    for(int i = props.length; i > 0; i--) {
      bProps = new $colon$colon(props[i - 1], bProps);
    }
    return bProps;
  }
}
