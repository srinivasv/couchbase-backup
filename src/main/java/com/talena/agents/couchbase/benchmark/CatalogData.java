package com.talena.agents.couchbase.benchmark;

public class CatalogData {
  private long vBucketUuid;
  private long lastSeqNo;

  public long getvBucketUuid() {
    return vBucketUuid;
  }

  public void setvBucketUuid(long vBucketUuid) {
    this.vBucketUuid = vBucketUuid;
  }

  public long getLastSeqNo() {
    return lastSeqNo;
  }

  public void setLastSeqNo(long lastSeqNo) {
    this.lastSeqNo = lastSeqNo;
  }

  @Override
  public String toString() {
    return String.format("{vBucket uuid = %d, last seq no = %d}", vBucketUuid, lastSeqNo);
  }
}
