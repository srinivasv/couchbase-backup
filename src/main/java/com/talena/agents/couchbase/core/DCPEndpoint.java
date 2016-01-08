package com.talena.agents.couchbase.core;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.dcp.StreamEndMessage;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import rx.functions.Action1;
import rx.functions.Func1;
import rx.Observable;

public class DCPEndpoint {
  private final static Logger logger = Logger.getLogger(DCPEndpoint.class);

  private final ClusterFacade core;
  private final String bucket;
  private final Map<Short, AbstractPartition> state;
  private final Map<Short, PartitionStats> stats;
  private AtomicReference<DCPConnection> connection;

  public DCPEndpoint(final ClusterFacade core, final String bucket) {
    this.core = core;
    this.bucket = bucket;
    this.state = new HashMap<Short, AbstractPartition>();
    this.stats = new HashMap<Short, PartitionStats>();
  }

  public void openConnection(final String name, Map<String, String> props) {
    if (connection != null) {
      return;
    }

    logger.info(String.format("New dcp connection: {name=%s, props=%s}",
      name, props));
    connection = new AtomicReference<DCPConnection>();
    OpenConnectionResponse response = core.<OpenConnectionResponse>send(
      new OpenConnectionRequest(name, bucket)).single().toBlocking().first();
    connection.compareAndSet(null, response.connection());
  }

  public Observable<DCPRequest> streamPartitions(
    final Map<Short, PartitionSpec> spec) {

    assertValidConnection();
    return stream(
      Observable
        .from(spec.values())
        .doOnNext(new Action1<PartitionSpec>() {
          @Override
          public void call(PartitionSpec s) {
            recordNewPartitionRequest(s);
          }
        })
      );
  }

  public Observable<DCPRequest> retryStalePartitions(
    final Map<Short, PartitionSpec> spec) {

    assertValidConnection();
    return stream(
      Observable
        .from(spec.values())
        .doOnNext(new Action1<PartitionSpec>() {
          @Override
          public void call(PartitionSpec s) {
            recordRetryStalePartitionRequest(s);
          }
        })
      );
  }

  public Observable<DCPRequest> retryOpenPartitions() {
    assertValidConnection();
    return stream(
      openPartitions()
        .map(new Func1<OpenPartition, PartitionSpec>() {
          @Override
          public PartitionSpec call(OpenPartition open) {
            return new PartitionSpec(open.id(), open.spec().uuid(),
              open.lastSeenSeqno(), open.spec().endSeqno(),
              open.lastSnapshotStartSeqno(), open.lastSnapshotEndSeqno());
          }
        })
        .doOnNext(new Action1<PartitionSpec>() {
          @Override
          public void call(PartitionSpec s) {
            recordRetryOpenPartitionRequest(s);
          }
        })
      );
  }

  public Observable<OpenPartition> openPartitions() {
    return allPartitions()
      .filter(new Func1<AbstractPartition, Boolean>() {
        @Override
        public Boolean call(AbstractPartition p) {
          return p.type() == AbstractPartition.Type.Open;
        }
      })
      .map(new Func1<AbstractPartition, OpenPartition>() {
        @Override
        public OpenPartition call(AbstractPartition p) {
          return (OpenPartition) p;
        }
      });
  }

  public Observable<ClosedPartition> closedPartitions() {
    return allPartitions()
      .filter(new Func1<AbstractPartition, Boolean>() {
        @Override
        public Boolean call(AbstractPartition p) {
          return p.type() == AbstractPartition.Type.Closed;
        }
      })
      .map(new Func1<AbstractPartition, ClosedPartition>() {
        @Override
        public ClosedPartition call(AbstractPartition p) {
          return (ClosedPartition) p;
        }
      });
  }

  public Observable<StalePartition> stalePartitions() {
    return allPartitions()
      .filter(new Func1<AbstractPartition, Boolean>() {
        @Override
        public Boolean call(AbstractPartition p) {
          return p.type() == AbstractPartition.Type.Stale;
        }
      })
      .map(new Func1<AbstractPartition, StalePartition>() {
        @Override
        public StalePartition call(AbstractPartition p) {
          return (StalePartition) p;
        }
      });
  }

  public Observable<PartitionStats> stats() {
    return Observable.from(stats.values());
  }

  private void assertValidConnection() {
    if (connection == null) {
      String msg = "DCP connection parameter is null";
      logger.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  private void assertStalePartition(final short id) {
    if (state.get(id) == null) {
      String msg = String.format("Partition %d does not exist.", id);
      logger.error(msg);
      throw new IllegalArgumentException(msg);
    }

    AbstractPartition.Type type = state.get(id).type();
    if (type != AbstractPartition.Type.Stale) {
      String msg = String.format(
        "Partition %s is not stale. It is in state %s.", id, type);
      logger.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  private void recordNewPartitionRequest(PartitionSpec s) {
    logger.info(String.format("New partition request: %s", s));
    state.put(s.id(), new OpenPartition(s.id(), s));
    stats.put(s.id(), new PartitionStats(s.id()));
  }

  private void recordRetryStalePartitionRequest(PartitionSpec s) {
    assertStalePartition(s.id());
    logger.info(String.format("Retry stale partition request: %s", s));
    state.put(s.id(), new OpenPartition(s.id(), s));
    stats.get(s.id()).increment(PartitionStats.Stat.NUM_RETRIES_REASON_STALE);
  }

  private void recordRetryOpenPartitionRequest(PartitionSpec s) {
    logger.info(String.format("Retry open partition request: %s", s));
    OpenPartition open = (OpenPartition) state.get(s.id());
    state.put(s.id(), new OpenPartition(open, s));
    stats.get(s.id()).increment(PartitionStats.Stat.NUM_RETRIES_REASON_OPEN);
  }

  private Observable<DCPRequest> stream(Observable<PartitionSpec> spec) {
    return spec
      .flatMap(new Func1<PartitionSpec, Observable<StreamRequestResponse>>() {
        @Override
        public Observable<StreamRequestResponse> call(final PartitionSpec s) {
          return core.send(
            new StreamRequestRequest(s.id(), s.uuid(), s.startSeqno(),
              s.endSeqno(), s.snapshotStartSeqno(), s.snapshotEndSeqno(),
              bucket));
        }
      })
      .doOnNext(new Action1<StreamRequestResponse>() {
        @Override
        public void call(final StreamRequestResponse resp) {
          handleStreamRequestResponse(resp);
        }
      })
      .toList()
      .flatMap(new Func1<List<StreamRequestResponse>,
        Observable<DCPRequest>>() {
        @Override
        public Observable<DCPRequest> call(final List<StreamRequestResponse> l) {
          return connection.get().subject();
        }
      })
      .doOnNext(new Action1<DCPRequest>() {
        @Override
        public void call(final DCPRequest req) {
          updatePartitionState(req);
        }
      })
      .filter(new Func1<DCPRequest, Boolean>() {
        @Override
        public Boolean call(final DCPRequest req) {
          return req instanceof MutationMessage || req instanceof RemoveMessage;
        }
      });
  }

  private void handleStreamRequestResponse(StreamRequestResponse resp) {
    final short id = resp.partition();
    switch (resp.status()) {
      case SUCCESS:
      {
        OpenPartition open = (OpenPartition) state.get(id);
        open.failoverLog(resp.failoverLog());
        logger.info(String.format("Stream request successful: %s", open));
      }
      break;

      case ROLLBACK:
      {
        OpenPartition open = (OpenPartition) state.get(id);
        long rollbackSeqno = resp.rollbackToSequenceNumber();
        StalePartition stale = new StalePartition(id, open.spec(),
          rollbackSeqno);
        state.put(id, stale);
        logger.warn(String.format("Stream request failed: %s", stale));
      }
      break;

      default:
      String msg = String.format("Stream request failed: %s", resp);
      logger.error(msg);
      throw new RuntimeException(msg);
    }
  }

  private void updatePartitionState(final DCPRequest req) {
    final short id = req.partition();
    if (req instanceof SnapshotMarkerMessage) {
      SnapshotMarkerMessage msg = (SnapshotMarkerMessage) req;
      recordSnapshotMarker(id, msg);
    } else if (req instanceof StreamEndMessage) {
      StreamEndMessage msg = (StreamEndMessage) req;
      recordStreamEnd(id, msg);
    } else if (req instanceof MutationMessage) {
      MutationMessage msg = (MutationMessage) req;
      recordMutation(id, msg);
    } else if (req instanceof RemoveMessage) {
      RemoveMessage msg = (RemoveMessage) req;
      recordRemove(id, msg);
    } else {
      String msg = String.format("Unexpected DCP request: %s", req);
      logger.error(msg);
      throw new RuntimeException(msg);
    }
  }

  private void recordSnapshotMarker(final short id,
    final SnapshotMarkerMessage msg) {

    OpenPartition open = (OpenPartition) state.get(id);
    open.lastSnapshotStartSeqno(msg.startSequenceNumber());
    open.lastSnapshotEndSeqno(msg.endSequenceNumber());
    stats.get(id).increment(PartitionStats.Stat.NUM_SNAPSHOTS);
  }

  private void recordStreamEnd(final short id, final StreamEndMessage msg) {
    OpenPartition open = (OpenPartition) state.get(id);
    ClosedPartition closed = new ClosedPartition(open, msg.reason());
    state.put(id, closed);
    logger.info(String.format("Partition closed: %s", closed));
  }

  private void recordMutation(final short id, final MutationMessage msg) {
    OpenPartition open = (OpenPartition) state.get(id);
    open.lastSeenSeqno(msg.bySequenceNumber());
    stats.get(id).increment(PartitionStats.Stat.NUM_MUTATIONS);
  }

  private void recordRemove(final short id, final RemoveMessage msg) {
    OpenPartition open = (OpenPartition) state.get(id);
    open.lastSeenSeqno(msg.bySequenceNumber());
    stats.get(id).increment(PartitionStats.Stat.NUM_DELETIONS);
  }

  private Observable<AbstractPartition> allPartitions() {
    return Observable.from(state.values());
  }
}
