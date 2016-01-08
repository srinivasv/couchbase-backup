package com.talena.agents.couchbase.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class CouchbaseRollbackRecord implements Writable {
  private short partition;
  private long uuid;
  private long seqno;

  public CouchbaseRollbackRecord() {
  }

  public short partition() {
    return partition;
  }

  public long uuid() {
    return uuid;
  }

  public long seqno() {
    return seqno;
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }
}
