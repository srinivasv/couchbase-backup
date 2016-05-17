package com.talena.agents.couchbase.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.VersionedWritable;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.commons.constants.CouchbaseConstants;

/**
 * Short record contains metadata fields of a record.
 * <p>
 * It implements Writable interface
 * for serialization. Since serialized objects are not to be compared,
 * WritableComparable is not required.
 * <p>
 * Implementing a comparable is not required for now because this class
 * will not be passed to the reducer.
 */
public class CouchbaseShortRecord extends CouchbaseRecord {

  protected short partition;
  protected long partitionUuid;
  protected long seqno;
  private String key;
  protected static final int LENGTH = Short.SIZE + Long.SIZE + 250;
  
  /**
   * Use {@link #create(MutationMessage)}, {@link #create(RemoveMessage)}.
   */
  protected CouchbaseShortRecord() {
    super(Type.UPDATE);
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

  public static CouchbaseShortRecord create() {
    CouchbaseShortRecord rec = new CouchbaseShortRecord();
    rec.partition = -1;
    rec.partitionUuid(-1L);
    rec.recType = Type.INSERT;
    return rec;
  }

  protected void set(MutationMessage msg) {
    this.key = msg.key();
    this.seqno = msg.bySequenceNumber();
    this.partition = msg.partition();
  }

  protected void set(RemoveMessage msg) {
    this.seqno = msg.bySequenceNumber();
    this.recType = Type.DELETE;
    this.partition = msg.partition();
  }

  public long seqno() {
    return seqno;
  }

  public String key() {
    return key;
  }

  public void key(String key) {
    this.key = key;
  }

  public Type recType() {
    return recType;
  }

  public short partition() {
    return partition;
  }

  public long partitionUuid() {
    return partitionUuid;
  }

  public void partitionUuid(long partitionUuid) {
    this.partitionUuid = partitionUuid;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(CouchbaseConstants.TYPE_SERIAL_VERSION);
    switch (CouchbaseConstants.TYPE_SERIAL_VERSION) {
      case 1:
        writeV1(out);
        break;
      default:
        throw new IOException(String.format("Invalid serial version %s",
            String.valueOf(CouchbaseConstants.TYPE_SERIAL_VERSION)));
    }
  }

  private void writeV1(DataOutput out) throws IOException {
    out.writeShort(this.partition);
    out.writeLong(this.partitionUuid);
    out.writeLong(this.seqno);
    out.writeUTF(this.recType.name());
    out.writeUTF(this.key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    byte version = in.readByte();
    if (version > CouchbaseConstants.TYPE_SERIAL_VERSION) {
      throw new VersionMismatchException(CouchbaseConstants.TYPE_SERIAL_VERSION,
          version);
    }
    switch (version) {
      case 1:
        readFieldsV1(in);
        break;
      default:
        throw new IOException(String.format("Invalid serial version %s",
            String.valueOf(version)));
    }
  }

  private void readFieldsV1(DataInput in) throws IOException {
    this.partition = in.readShort();
    this.partitionUuid = in.readLong();
    this.seqno = in.readLong();
    Enum.valueOf(CouchbaseShortRecord.Type.class, in.readUTF());
    this.key = in.readUTF();
  }

  public void set(CouchbaseShortRecord record) {
    this.seqno = record.seqno;
    this.recType = record.recType;
    this.partition = record.partition;
    this.key = record.key;
    this.partitionUuid = record.partitionUuid;
  }

  /**
   * Compares all fields.
   */
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof CouchbaseShortRecord))
      return false;
    CouchbaseShortRecord that = (CouchbaseShortRecord) o;
    if (this.recType == that.recType && this.partition == that.partition
        && this.seqno == that.seqno
        && this.partitionUuid == that.partitionUuid) {
      if ((this.key == null && that.key == null) || this.key.equals(that.key))
        return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return new StringBuilder("CouchbaseShortRecord [partition=")
        .append(partition).append(", seqno=").append(seqno).append(", key=")
        .append(key).append(", recType=").append(recType)
        .append(", partitionUuid=").append(this.partitionUuid).append("]")
        .toString();
  }
  
  public int length() {
    return LENGTH;
  }
  
  public void clear() {
    partition = 0;
    seqno = 0;
    key = null;
  }
}
