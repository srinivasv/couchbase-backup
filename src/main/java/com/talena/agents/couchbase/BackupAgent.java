package com.talena.agents.couchbase;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.kv.MutationToken;

import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.ClosedPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import rx.functions.Action1;

public class BackupAgent {
  public final String BACKUP_STREAM_NAME = "backup-stream";
  public final String MAINLINE = "ml";

  private final String[] nodes;
  private final String bucket;
  private final String password;

  private final Map<String, Map<Short, PartitionChanges>> backups;
  private final Map<String, Map<Short, ClosedPartition>> catalog;

  public BackupAgent(final String[] nodes, final String bucket,
    final String password) {

    this.nodes = nodes;
    this.bucket = bucket;
    this.password = password;

    this.backups = new HashMap<String, Map<Short, PartitionChanges>>();
    this.catalog = new HashMap<String, Map<Short, ClosedPartition>>();
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

  public Map<Short, PartitionSpec> partitionsWithChanges() {
    Random r = new Random();

    // Connect to the couchbase cluster
    CouchbaseFacade facade = new CouchbaseFacade(nodes, bucket, password);
    facade.openBucket();

    Map<Short, PartitionSpec> spec = new HashMap<Short, PartitionSpec>();
    int success = 0, error = 0, rollback = 0;
    for (Entry<Short, Long> entry : facade.currentHighSeqnos().entrySet()) {
      short id = entry.getKey();
      long highSeqno = entry.getValue();

      long uuid = 0;
      long startSeqno = 0;
      long endSeqno = highSeqno;

      if (catalog.containsKey(MAINLINE)) {
        ClosedPartition state = catalog.get(MAINLINE).get(id);
        if (state != null) {
          uuid = state.failoverLog().get(0).vbucketUUID();
          startSeqno = state.lastSeenSeqno();
        }
      }

      if (endSeqno == startSeqno) {
        System.out.println(String.format(
          "No changes for partition %d.", id));
        continue;
      }

      int n = r.nextInt(2);
      //int n = r.nextInt(3);
      if (n == 0) {
        spec.put(id, new PartitionSpec(id, uuid, startSeqno, endSeqno,
          startSeqno, startSeqno));
        success++;
      } else if (n == 1) {
        spec.put(id, new PartitionSpec(id, 0, 100, 200, 100, 100));
        rollback++;
      } else if (n == 2) {
        spec.put(id, new PartitionSpec(id, uuid, 10, 9, 10, 10));
        error++;
      }

      /*
      if (id > 0) {
        continue;
      }
      spec.put(id, new PartitionSpec(id, 0, 0, 0xffffffff, 0, 0));
      */
    }
    System.out.println();
    System.out.println("success: " + success);
    System.out.println("error: " + error);
    System.out.println("rollback: " + rollback);
    return spec;
  }

  public void backup(final String snapId) {
    // Determine which partitions have new changes since the last backup
    Map<Short, PartitionSpec> spec = partitionsWithChanges();

    // Backup the changes
    PartitionMover mover = new PartitionMover(nodes, bucket, password, spec);
    Map<Short, PartitionChanges> changes = mover.backup();

    // Add the new snapshot and update the mainline
    snapshotChanges(snapId, changes);
  }

  public PartitionChanges partitionChangesFor(final String snapId,
    final short partition) {

    return backups.get(snapId).get(partition);
  }

  public ClosedPartition partitionStateFor(final String snapId,
    final short partition) {

    return catalog.get(snapId).get(partition);
  }

  private void snapshotChanges(String snapId,
    Map<Short, PartitionChanges> changes) {

    System.out.println(String.format("Creating snapshot %s", snapId));
    Map<Short, ClosedPartition> props = new HashMap<Short, ClosedPartition>();

    // Update mainline
    if (changes.size() > 0) {
      for (Short partition : changes.keySet()) {
        props.put(partition, changes.get(partition).state());
      }
      catalog.put(MAINLINE, props);
      backups.put(MAINLINE, changes);
    }

    // Create snapshot
    catalog.put(snapId, props);
    backups.put(snapId, changes);
  }
}
