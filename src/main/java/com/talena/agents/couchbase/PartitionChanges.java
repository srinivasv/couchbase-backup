package com.talena.agents.couchbase;

import com.talena.agents.couchbase.core.ClosedPartition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PartitionChanges {
  private final List<SeqnoKeyPair> seqnoKeyPairs;
  private final List<Mutation> mutations;
  private ClosedPartition state;

  public PartitionChanges() {
    seqnoKeyPairs = new LinkedList<SeqnoKeyPair>();
    mutations = new LinkedList<Mutation>();
  }

  public List<SeqnoKeyPair> seqnoKeyPairs() {
    return seqnoKeyPairs;
  }

  public List<Mutation> mutations() {
    return mutations;
  }

  public ClosedPartition state() {
    return state;
  }

  public void state(ClosedPartition state) {
    this.state = state;
  }
}
