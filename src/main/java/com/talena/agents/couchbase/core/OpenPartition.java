package com.talena.agents.couchbase.core;

import com.couchbase.client.core.message.dcp.FailoverLogEntry;

import com.talena.agents.couchbase.core.AbstractPartition.Type;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import rx.Observable;
import rx.functions.Func1;

/**
 * <p>A partition that was successfully opened but hasn't been closed yet.</p>
 *
 * <p>All of the partition's data may not have been streamed if the connection
 * was lost due to some reason.</p>
 */
public class OpenPartition extends AbstractPartition {
  // DCP state required for retries of the current stream and any subsequent
  // incremental streams. Must be persisted to stable storage after each
  // successful streaming operation.
  private List<FailoverLogEntry> failoverLog;
  private long lastSeenSeqno;
  private long lastSnapshotStartSeqno;
  private long lastSnapshotEndSeqno;

  public OpenPartition(final short id, final PartitionSpec spec) {
    super(id, spec);
  }

  public OpenPartition(final OpenPartition open) {
    super(open.id(), open.spec());
    this.failoverLog = open.failoverLog();
    this.lastSeenSeqno = open.lastSeenSeqno();
    this.lastSnapshotStartSeqno = open.lastSnapshotStartSeqno();
    this.lastSnapshotEndSeqno = open.lastSnapshotEndSeqno();
  }

  public OpenPartition(final OpenPartition open, final PartitionSpec spec) {
    super(open.id(), spec);
    this.failoverLog = open.failoverLog();
    this.lastSeenSeqno = open.lastSeenSeqno();
    this.lastSnapshotStartSeqno = open.lastSnapshotStartSeqno();
    this.lastSnapshotEndSeqno = open.lastSnapshotEndSeqno();
  }

  @Override
  public Type type() {
    return Type.Open;
  }

  public List<FailoverLogEntry> failoverLog() {
    return failoverLog;
  }

  public void failoverLog(List<FailoverLogEntry> failoverLog) {
    this.failoverLog = failoverLog;
  }

  public long lastSeenSeqno() {
    return lastSeenSeqno;
  }

  public void lastSeenSeqno(long lastSeenSeqno) {
    this.lastSeenSeqno = lastSeenSeqno;
  }

  public long lastSnapshotStartSeqno() {
    return lastSnapshotStartSeqno;
  }

  public void lastSnapshotStartSeqno(long lastSnapshotStartSeqno) {
    this.lastSnapshotStartSeqno = lastSnapshotStartSeqno;
  }

  public long lastSnapshotEndSeqno() {
    return lastSnapshotEndSeqno;
  }

  public void lastSnapshotEndSeqno(long lastSnapshotEndSeqno) {
    this.lastSnapshotEndSeqno = lastSnapshotEndSeqno;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("OpenPartition{id=").append(id());
    sb.append(", failoverLog=").append(StringUtils.join(
      Observable.from(failoverLog())
        .map(new Func1<FailoverLogEntry, String>() {
          @Override
          public String call(FailoverLogEntry entry) {
            return String.format("FailoverLogEntry{uuid=%d, seqno=%d}",
            entry.vbucketUUID(), entry.sequenceNumber());
          }
        })
        .toList()
        .toBlocking()
        .first()
        ));
    sb.append(", lastSeenSeqno=").append(lastSeenSeqno());
    sb.append(", lastSnapshotStartSeqno=").append(lastSnapshotStartSeqno());
    sb.append(", lastSnapshotEndSeqno=").append(lastSnapshotEndSeqno());
    sb.append("}");

    return sb.toString();
  }
}
