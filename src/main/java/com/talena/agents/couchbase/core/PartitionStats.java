package com.talena.agents.couchbase.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class PartitionStats {

  public enum Stat {
    NUM_SNAPSHOTS("num_snapshots"),
    NUM_MUTATIONS("num_mutations"),
    NUM_DELETIONS("num_deletions"),
    NUM_RETRIES_REASON_OPEN("num_retries_reason_open"),
    NUM_RETRIES_REASON_STALE("num_retries_reason_stale");

    final String name;

    Stat(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private final short id;
  private final Map<Stat, Long> stats;

  public PartitionStats(final short id) {
    this.id = id;
    this.stats = new HashMap<Stat, Long>();
    for (Stat s : Stat.values()) {
      stats.put(s, 0L);
    }
  }

  public short id() {
    return id;
  }

  public Map<Stat, Long> stats() {
    return stats;
  }

  public void increment(Stat stat) {
    final long val = stats.get(stat);
    stats.put(stat, val + 1);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("PartitionStats{id=").append(id());
    sb.append(", stats=").append(stats);
    sb.append("}");

    return sb.toString();
  }
}
