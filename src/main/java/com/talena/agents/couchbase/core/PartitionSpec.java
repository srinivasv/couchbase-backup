package com.talena.agents.couchbase.core;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>The parameters required to initiate a stream for a given partition</p>
 *
 * <p>Refer to DCP documentation for an exposition of the class attributes.<\p>
 */
public class PartitionSpec {
  private final short id;
  private final long uuid;
  private final long startSeqno;
  private final long endSeqno;
  private final long snapshotStartSeqno;
  private final long snapshotEndSeqno;

  public PartitionSpec(final short id, final long uuid, final long startSeqno,
    final long endSeqno, final long snapshotStartSeqno,
    final long snapshotEndSeqno) {

    this.id = id;
    this.uuid = uuid;
    this.startSeqno = startSeqno;
    this.endSeqno = endSeqno;
    this.snapshotStartSeqno = snapshotStartSeqno;
    this.snapshotEndSeqno = snapshotEndSeqno;
  }

  public short id() {
    return id;
  }

  public long uuid() {
    return uuid;
  }

  public long startSeqno() {
    return startSeqno;
  }

  public long endSeqno() {
    return endSeqno;
  }

  public long snapshotStartSeqno() {
    return snapshotStartSeqno;
  }

  public long snapshotEndSeqno() {
    return snapshotEndSeqno;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("PartitionSpec{id=").append(id());
    sb.append(", uuid=").append(uuid());
    sb.append(", startSeqno=").append(startSeqno());
    sb.append(", endSeqno=").append(endSeqno());
    sb.append(", snapshotStartSeqno=").append(snapshotStartSeqno());
    sb.append(", snapshotEndSeqno=").append(snapshotEndSeqno());
    sb.append("}");

    return sb.toString();
  }
}
