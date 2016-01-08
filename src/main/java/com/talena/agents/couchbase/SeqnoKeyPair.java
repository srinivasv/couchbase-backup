package com.talena.agents.couchbase;

public class SeqnoKeyPair {
  private final long seqno;
  private final String key;

  public SeqnoKeyPair(final long seqno, final String key) {
    this.seqno = seqno;
    this.key = key;
  }

  public long seqno() {
    return seqno;
  }

  public String key() {
    return key;
  }

  public String toString() {
    return String.format("%d : %s", seqno, key);
  }
}
