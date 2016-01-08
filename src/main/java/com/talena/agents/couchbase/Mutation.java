package com.talena.agents.couchbase;

public class Mutation {

  public enum Type {
    mutation,
    remove;
  }

  private final Type type;
  private final String key;
  private final long seqno;
  private final String value;

  public Mutation(final Type type, final long seqno, final String key,
    final String value) {

    if (type != Type.mutation) {
      throw new IllegalArgumentException("Type must be 'mutation'");
    }
    this.type = type;
    this.seqno = seqno;
    this.key = key;
    this.value = value;
  }

  public Mutation(final Type type, final long seqno, final String key) {
    if (type != Type.remove) {
      throw new IllegalArgumentException("Type must be 'remove'");
    }
    this.type = type;
    this.seqno = seqno;
    this.key = key;
    this.value = null;
  }

  public Type type() {
    return type;
  }

  public long seqno() {
    return seqno;
  }

  public String key() {
    return key;
  }

  public String value() {
    return value;
  }

  public String toString() {
    return String.format("%s, %d, %s, %s", type, seqno, key, value);
  }
}
