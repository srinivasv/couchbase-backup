package com.talena.agents.couchbase.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.core.CouchbaseShortRecord.Type;

/**
 * Long record contains metadata and data.
 * 
 * <p>It implements Writable interface
 * for serialization. Since serialized objects are not to be compared,
 * WritableComparable is not required.
 * 
 * @author Unascribed
 */
public class CouchbaseLongRecord extends CouchbaseShortRecord
    implements Writable {
  
  private static final Log logger = LogFactory.getLog(
      CouchbaseLongRecord.class);   
  
  private byte[] content;
  private int contentLen = -1;  
  protected int expiration;
  protected long revisionSeqNo;
  protected int lockTime;
  protected long cas;
  protected int flags;  
  
  
  public static CouchbaseLongRecord create(MutationMessage msg) {
    CouchbaseLongRecord rec = new CouchbaseLongRecord();
    rec.set(msg);
    rec.content = msg.content().array();
    rec.expiration = msg.expiration();
    rec.revisionSeqNo = msg.revisionSequenceNumber();
    rec.lockTime = msg.lockTime();
    rec.cas = msg.cas();
    rec.flags = msg.flags();
    if (rec.content != null) {
      rec.contentLen = rec.content.length;
    }
    return rec;
  }
  
  public static CouchbaseLongRecord create(RemoveMessage msg) {
    CouchbaseLongRecord rec = new CouchbaseLongRecord();
    rec.set(msg);
    rec.revisionSeqNo = msg.revisionSequenceNumber();
    rec.cas = msg.cas();
    return rec;
  } 
  
  /**
   * @return The content
   */
  public byte[] content() {
    return content;
  }
  

  @Override
  public void write(DataOutput out) throws IOException {
    try {
      super.write(out);
      out.writeInt(this.expiration);
      out.writeLong(this.revisionSeqNo);
      out.writeInt(this.lockTime);
      out.writeLong(this.cas);
      out.writeInt(flags);
      out.writeInt(this.contentLen);
      if (this.contentLen != -1) {
        out.write(this.content);
      }
    } catch(IOException e) {
      logger.error("Error in record serialization.", e);
      throw e;
    }
   }

  @Override
  public void readFields(DataInput in) throws IOException {

    try {
      super.readFields(in);
      this.expiration = in.readInt();
      this.revisionSeqNo = in.readLong();
      this.lockTime = in.readInt();
      this.cas = in.readLong();
      this.flags = in.readInt();
      this.contentLen = in.readInt();
      this.content = null;
      if (this.contentLen != -1) {
        this.content = new byte[this.contentLen];
        in.readFully(this.content);
      }
    } catch(IOException e) {
      logger.error("Error in record deserialization.", e);
      throw e;
    }    
  }
  
  public void set(CouchbaseLongRecord record) {
    
    super.set(record);
    this.recType = Type.UPDATE;
    this.expiration = record.expiration;
    this.revisionSeqNo = record.revisionSeqNo;
    this.lockTime = record.lockTime;
    this.cas = record.cas;
    this.flags = record.flags;
    this.content = record.content;
    this.contentLen = -1;
    if (this.content != null) {
      this.contentLen = this.content.length;
    }       
  }
  
  /**
   * @return The expiration
   */
  public int expiration() {
    return expiration;
  }
  /**
   * @return The revisionSeqNo
   */
  public long revisionSeqNo() {
    return revisionSeqNo;
  }
  /**
   * @return The lockTime
   */
  public int lockTime() {
    return lockTime;
  }
  /**
   * @return The cas
   */
  public long cas() {
    return cas;
  }
  /**
   * @return The flags
   */
  public int flags() {
    return flags;
  }
  
}
