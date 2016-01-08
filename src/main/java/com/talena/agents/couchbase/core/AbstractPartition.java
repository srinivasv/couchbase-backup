package com.talena.agents.couchbase.core;

public abstract class AbstractPartition {

  public enum Type {
    // The partition was successfully opened but hasn't been closed yet. All of
    // the partition's data may not be streamed if the connection is lost.
    Open,

    // The partition was successfully opened and is closed now. All of the
    // partition's data has been streamed.
    Closed,

    // An attempt was made to open the partition but it turned out to be stale.
    // The partition needs to be rolled back to an earlier sequence number.
    Stale;
  }

  private final short id;

  // The spec (start seqno, end seqno, etc.) for streaming the partition.
  private PartitionSpec spec;

  public AbstractPartition(final short id, final PartitionSpec spec) {
    this.id = id;
    this.spec = spec;
  }

  public abstract Type type();

  public short id() {
    return id;
  }

  public PartitionSpec spec() {
    return spec;
  }
}
