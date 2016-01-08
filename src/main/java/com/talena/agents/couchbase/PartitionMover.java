package com.talena.agents.couchbase;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;

import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.ClosedPartition;
import com.talena.agents.couchbase.core.DCPEndpoint;
import com.talena.agents.couchbase.core.OpenPartition;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.PartitionStats;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import rx.functions.Action1;

public class PartitionMover {
  private final String[] nodes;
  private final String bucket;
  private final String password;
  private final Map<Short, PartitionSpec> spec;

  private Map<Short, PartitionChanges> changes;
  private Map<Short, List<SeqnoKeyPair>> seqnoKeyPairs;
  private Map<Short, List<Mutation>> mutations;

  public PartitionMover(final String[] nodes, final String bucket,
    final String password, final Map<Short, PartitionSpec> spec) {

    this.nodes = nodes;
    this.bucket = bucket;
    this.password = password;
    this.spec = spec;
  }

  public String[] nodes() {
    return nodes;
  }

  public String bucket() {
    return bucket;
  }

  public String password() {
    return password;
  }

  public Map<Short, PartitionSpec> spec() {
    return spec;
  }

  public Map<Short, PartitionChanges> backup() {
    if (spec.size() == 0) {
      System.out.println("Nothing to backup. Exiting.");
      return new HashMap<Short, PartitionChanges>(spec.size());
    }

    changes = new HashMap<Short, PartitionChanges>(spec.size());
    seqnoKeyPairs = new HashMap<Short, List<SeqnoKeyPair>>(spec.size());
    mutations = new HashMap<Short, List<Mutation>>(spec.size());
    for (PartitionSpec s : spec.values()) {
      short id = s.id();
      changes.put(id, new PartitionChanges());
      seqnoKeyPairs.put(id, changes.get(id).seqnoKeyPairs());
      mutations.put(id, changes.get(id).mutations());
    }

    CouchbaseFacade couchbase = new CouchbaseFacade(nodes, bucket, password);
    couchbase.openBucket();

    DCPEndpoint dcp = couchbase.dcpEndpoint();
    dcp.openConnection("test", new HashMap<String, String>());
    dcp.streamPartitions(spec)
      .toBlocking()
      .forEach(new Action1<DCPRequest>() {
        @Override
        public void call(DCPRequest req) {
          handleDCPRequest(req);
        }
      });

    dcp.closedPartitions()
      .toBlocking()
      .forEach(new Action1<ClosedPartition>() {
        @Override
        public void call(ClosedPartition p) {
          changes.get(p.id()).state(p);
        }
      });

    System.out.println("Open partitions");
    dcp.openPartitions()
      .toBlocking()
      .forEach(new Action1<OpenPartition>() {
        @Override
        public void call(OpenPartition p) {
          System.out.println(p);
        }
      });

    System.out.println();
    dcp.stats()
      .toBlocking()
      .forEach(new Action1<PartitionStats>() {
        @Override
        public void call(PartitionStats stats) {
          System.out.println(stats);
        }
      });

    return changes;
  }

  private void handleDCPRequest(final DCPRequest req) {
    if (req instanceof MutationMessage) {
      MutationMessage msg = (MutationMessage) req;
      handleMutation(msg, msg.partition());
    } else if (req instanceof RemoveMessage) {
      RemoveMessage msg = (RemoveMessage) req;
      handleRemove(msg, msg.partition());
    }
  }

  private void handleMutation(final MutationMessage msg, short partition) {
    seqnoKeyPairs.get(partition).add(new SeqnoKeyPair(
      msg.bySequenceNumber(),
      msg.key()));
    mutations.get(partition).add(new Mutation(
      Mutation.Type.mutation,
      msg.bySequenceNumber(),
      msg.key(),
      msg.content().toString(StandardCharsets.UTF_8)));
  }

  private void handleRemove(final RemoveMessage msg, short partition) {
    seqnoKeyPairs.get(partition).add(new SeqnoKeyPair(
      msg.bySequenceNumber(),
      msg.key()));
    mutations.get(partition).add(new Mutation(
      Mutation.Type.mutation,
      msg.bySequenceNumber(),
      msg.key()));
  }
}
