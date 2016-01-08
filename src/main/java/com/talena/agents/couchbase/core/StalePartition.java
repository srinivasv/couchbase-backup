package com.talena.agents.couchbase.core;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>A partition for which an attempt was made to open but which turned out to
 * be stale.</p>
 *
 * <p>The partition needs to be rolled back to an earlier sequence number.
 * </p>
 */
public class StalePartition extends AbstractPartition {
  // The sequence number to which the partition must be rolled back to as
  // instructed by the server.
  private long rollbackSeqno;

  public StalePartition(final short id, final PartitionSpec spec,
    final long rollbackSeqno) {

    super(id, spec);
    this.rollbackSeqno = rollbackSeqno;
  }

  @Override
  public Type type() {
    return Type.Stale;
  }

  public long rollbackSeqno() {
    return rollbackSeqno;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("StalePartition{id=").append(id());
    sb.append(", rollbackSeqno=").append(rollbackSeqno());
    sb.append("}");

    return sb.toString();
  }
}
