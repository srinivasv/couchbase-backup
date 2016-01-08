package com.talena.agents.couchbase.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;

/**
 * Short record contains metadata fields of a record.
 * 
 * <p>It implements Writable interface
 * for serialization. Since serialized objects are not to be compared,
 * WritableComparable is not required.
 */
public class CouchbaseShortRecord implements Writable {
  
  public enum Type {
    INSERT,
    UPDATE,
    DELETE,
    ROLLBACK
  }
  
  protected short partitionId; 
  protected short uuid; 
  protected long seqNo;
  private String key;
  protected Type recType;
  
  public CouchbaseShortRecord(final short partitionId, final long seqNo,
    final String key, final Type recType) {
    this.partitionId = partitionId;
    this.seqNo = seqNo;
    this.key = key;
    this.recType = recType;
  }

  public CouchbaseShortRecord(final short partitionId, final String key,
    final long seqNo) {
    this.partitionId = partitionId;
    this.key = key;
    this.seqNo = seqNo;
  }

  public CouchbaseShortRecord() {
  }

  public static CouchbaseShortRecord create(MutationMessage msg) {
    CouchbaseShortRecord rec = new CouchbaseShortRecord();
    rec.set(msg);
    return rec;
  }
  
  public static CouchbaseShortRecord create(RemoveMessage msg) {
    CouchbaseShortRecord rec = new CouchbaseShortRecord();
    rec.set(msg);        
    return rec;
  }
  
  public static CouchbaseShortRecord create(long seqNo, int expiration, int revisionSeqNo,
      int lockTime, long cas, int flags, Type recType) {
    CouchbaseShortRecord rec = new CouchbaseShortRecord();
    rec.set(seqNo, expiration, revisionSeqNo, lockTime, cas, flags, recType);
    return rec;
  }
  
  protected void set(MutationMessage msg) {
    this.key = msg.key();
    this.seqNo = msg.bySequenceNumber();    
    this.partitionId = msg.partition();
  }
  
  protected void set(RemoveMessage msg) {
    this.seqNo = msg.bySequenceNumber();
    this.recType = Type.DELETE;
    this.partitionId = msg.partition();
  }
  
  protected void set(long seqNo, int expiration, int revisionSeqNo,
      int lockTime, long cas, int flags, Type recType) {
    
    this.seqNo = seqNo;    
    this.recType = recType;        
  }
  
  /**
   * @return The seqNo
   */
  public long seqNo() {
    return seqNo;
  }  

  /**
   * @return The key
   */
  public String key() {
    return key;
  }
  
  public void key(String key) {
    this.key = key;
  }

  public long uuid() {
    return this.uuid;
  }
  
  /**
   * @return The recType
   */
  public Type recType() {
    return recType;
  }

  public short partitionId() {
    return partitionId;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(this.partitionId);
    out.writeLong(this.seqNo);    
    UTF8.writeString(out, this.recType.name());
        
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    
    this.partitionId = in.readShort();
    this.seqNo = in.readLong();
    this.recType = Enum.valueOf(Type.class, UTF8.readString(in));
  }

  public void set(CouchbaseShortRecord record) {
    this.seqNo = record.seqNo;
    this.recType = record.recType;
    this.partitionId = record.partitionId;
  }

  @Override
  public String toString() {
    return String.format("(partitionId=%d, seqNo=%d, recType=%s)",
      partitionId, seqNo, recType);
  }
}
