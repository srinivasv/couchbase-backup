package com.talena.agents.couchbase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBackupService {
  private final String[] nodes = new String[] {"127.0.0.1"};
  private final String bucketName = "default";
  private final String password = "";
  private BackupAgent backupAgent;

  @Before
  public void setup() {
    backupAgent = new BackupAgent(nodes, bucketName, password);
  }

  @After
  public void cleanup() {
  }

  private void backup(String snapId) {
    backupAgent.backup(snapId);

    /*
    short vbucket = 0;
    PartitionChanges changes = backupAgent.partitionChangesFor(snapId, vbucket);
    if (changes == null) {
      System.out.println(String.format("No changes in %s.", snapId));
      return;
    }

    System.out.println();
    System.out.println(String.format("SnapId: %s, Props: %s", snapId,
      backupAgent.partitionStateFor(snapId, vbucket)));

    System.out.println();
    System.out.println("Mutation stream...");
    for (Mutation m : changes.mutations()) {
      System.out.println(m);
    }

    System.out.println();
    System.out.println("Seqno key pairs...");
    for (SeqnoKeyPair p : changes.seqnoKeyPairs()) {
      System.out.println(p);
    }
    */
  }

  @Test
  public void testSimpleBackup() {
    backup("full");
    backup("inc1");
    backup("inc2");
    backup("inc3");
    backup("inc4");
    backup("inc5");
    backup("inc6");
    backup("inc7");
    backup("inc8");
    backup("inc9");
  }
}
