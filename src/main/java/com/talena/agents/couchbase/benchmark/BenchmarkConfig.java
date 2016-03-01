package com.talena.agents.couchbase.benchmark;

public class BenchmarkConfig {
  public enum OP_TYPE {
    USAGE("Usage"),
    BACKUP_FULL("Full"),
    BACKUP_INCR("Incremental"),
    TIMED_RUN("Timed Run");

    private String string;

    OP_TYPE(String value) {
      this.string = value;
    }

    @Override
    public String toString() {
      return string;
    }
  };

  private String bucketName;
  private OP_TYPE operation;
  private long secondsWait;
  private String backupPath;
  private String nodesIp[];

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public OP_TYPE getOperation() {
    return operation;
  }

  public void setOperation(OP_TYPE operation) {
    this.operation = operation;
  }

  public long getSecondsWait() {
    return secondsWait;
  }

  public void setSecondsWait(long secondsWait) {
    this.secondsWait = secondsWait;
  }

  public String getBackupPath() {
    return backupPath;
  }

  public void setBackupPath(String backupPath) {
    this.backupPath = backupPath;
  }

  public String[] getNodesIp() {
    return nodesIp;
  }

  public void setNodesIp(String[] nodesIp) {
    this.nodesIp = nodesIp;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("BenchmarkConfig {bucket = ").append(getBucketName());
    sb.append(", operation = ").append(getOperation());
    if (getOperation() == OP_TYPE.TIMED_RUN) {
      sb.append(", wait seconds = ").append(getSecondsWait());
    }
    sb.append(", backup path = ").append(getBackupPath());
    sb.append(", nodes = [");
    boolean isFirst = true;
    for (String node : getNodesIp()) {
      if (!isFirst) {
        sb.append(", ");
      } else {
        isFirst = false;
      }
      sb.append(node);
    }
    sb.append("]");
    sb.append("}");

    return sb.toString();
  }
}
