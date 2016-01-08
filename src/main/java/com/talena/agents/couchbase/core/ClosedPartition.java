package com.talena.agents.couchbase.core;

import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.StreamEndMessage.Reason;

import com.talena.agents.couchbase.core.AbstractPartition.Type;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import rx.Observable;
import rx.functions.Func1;

/**
 * <p>A partition that was successfully opened and is closed now.</p>
 *
 * <p>All of the partition's data has been successfully streamed.</p>
 */
public class ClosedPartition extends OpenPartition {
  // The reason why the partition was closed
  private Reason reason;

  public ClosedPartition(OpenPartition open, Reason reason) {
    super(open);
    this.reason = reason;
  }

  @Override
  public Type type() {
    return Type.Closed;
  }

  public Reason reason() {
    return reason;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("ClosedPartition{id=").append(id());
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
    sb.append(", reason=").append(reason());
    sb.append("}");

    return sb.toString();
  }
}
