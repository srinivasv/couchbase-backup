package com.talena.agents.couchbase.benchmark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.PartitionChanges;
import com.talena.agents.couchbase.core.ClosedPartition;
import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.DCPEndpoint;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.PartitionStats;

import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.MathObservable;

public class BenchmarkBackup {
  private long bytesCount = 0;
  private long startTime = 0;
  private FileOutputStream fOutBackup = null;
  private DataOutputStream dataOutBackup = null;
  private Map<Short, PartitionChanges> changes;
  private long mutationCount = 0;

  public static void main(String[] args) {
    if (args.length < 5) {
      System.out.println("Usage: <bucket> <waitTimeSeconds> <backup-path> <Full/Incremental> <ipNodes>");

      return;
    }

    String bucket = args[0];
    long waitTimeSec = Long.parseLong(args[1]);
    String backupPath = args[2];
    String backupType = args[3];
    String nodes[] = new String[args.length - 4];
    boolean isFullBackup = true;

    System.arraycopy(args, 4, nodes, 0, args.length - 4);

    if (backupType.compareToIgnoreCase("Full") == 0) {
      System.out.print("Taking full backup of ");
    } else if (backupType.compareToIgnoreCase("Incremental") == 0) {
      isFullBackup = false;
      System.out.print("Taking incremental backup of ");
    } else {
      System.out.println("Usage: <bucket> <waitTimeSeconds> <backup-path> <Full/Incremental> <ipNodes>");

      return;
    }

    System.out.println("bucket: " + bucket
        + ". With wait time of: " + waitTimeSec + "(seconds), at " + backupPath);
    System.out.print("Connecting to nodes: ");
    for (String node : nodes) {
      System.out.print(node + " ");
    }
    System.out.println("");

    BenchmarkBackup app = new BenchmarkBackup();

    app.Run(bucket, waitTimeSec, nodes, backupPath, isFullBackup);
  }

  public void Run(String bucket, long waitTimeSec, String[] nodes, String backupPath, boolean isFullBackup) {
    CouchbaseFacade cbFacade = new CouchbaseFacade(nodes, bucket, "");
    cbFacade.openBucket();

    Map<Short, Long> highSeqNos = cbFacade.currentHighSeqnos();
    short changedPartitions[] = new short[1024];
    int changedPartitionsCount = 0;

    System.out.println("vBuckets high sequence numbers are ...");

    File metaFile = new File(backupPath + "meta.data");
    File backupFile = new File(backupPath + "backup.data" + (isFullBackup ? ".full" : ".incr"));
    FileInputStream fIn = null;
    FileOutputStream fOut = null;
    DataInputStream dataIn = null;
    DataOutputStream dataOut = null;
    Map<Short, Long> metaData = new HashMap<Short, Long>();

    if (!metaFile.exists()) {
      System.out.println(metaFile.getAbsolutePath() + " metafile does not exist. Can only do full backup.");
      isFullBackup = true;
    }

    try {
      if (!isFullBackup) {
        fIn = new FileInputStream(metaFile);
        dataIn = new DataInputStream(fIn);

        short key;
        long value;

        while (dataIn.available() != 0) {
          key = dataIn.readShort();
          value = dataIn.readLong();

          metaData.put(key, value);
        }

        fIn.close();
        dataIn.close();
      }

      fOut = new FileOutputStream(metaFile);
      dataOut = new DataOutputStream(fOut);

      System.out.println("Writing metadata to " + metaFile.getAbsolutePath());

      for (Map.Entry<Short, Long> highSeqNo : highSeqNos.entrySet()) {
        dataOut.writeShort(highSeqNo.getKey());
        dataOut.writeLong(highSeqNo.getValue());

        if (highSeqNo.getValue() != 0) {
          System.out.println(highSeqNo.getKey() + " " + highSeqNo.getValue());
          changedPartitions[changedPartitionsCount] = highSeqNo.getKey().shortValue();
          ++changedPartitionsCount;
        }
      }

      dataOut.close();
      fOut.close();

      fOutBackup = new FileOutputStream(backupFile);
      dataOutBackup = new DataOutputStream(fOutBackup);
    } catch (Exception e) {
      System.out.println(e);
    }

    DCPEndpoint dcp = cbFacade.dcpEndpoint();
    dcp.openConnection("streamConn", new HashMap<String, String>());

    Map<Short, PartitionSpec> specs = new HashMap<Short, PartitionSpec>();
    for (int i = 0; i < changedPartitionsCount; ++i) {
      short idChanged = changedPartitions[i];
      long endSeqNo = highSeqNos.get(idChanged);
      long startSeqNo = 0;

      if (metaData.get(idChanged) != null) {
        startSeqNo = metaData.get(idChanged); 
      }

      if (endSeqNo > startSeqNo) {
        PartitionSpec partSpec = new PartitionSpec(idChanged, 35730237730096L, startSeqNo, endSeqNo, startSeqNo, endSeqNo);
        //PartitionSpec partSpec = new PartitionSpec(idChanged, 0, 0, Long.MAX_VALUE, 0, 0);
        System.out.println("For partition " + idChanged + " range is " + startSeqNo + " to " + endSeqNo);
  
        specs.put(idChanged, partSpec);
      }
    }

    changes = new HashMap<Short, PartitionChanges>(specs.size());
    for (PartitionSpec s : specs.values()) {
      short id = s.id();
      changes.put(id, new PartitionChanges());
    }

    for (int i = 5; i >= 1; --i) {
      System.out.print("\rStarting in " + i + " seconds.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    System.out.println("");

    Subscription s = dcp.streamPartitions(specs)
        .subscribe(new Action1<DCPRequest>() {
        @Override
          public void call(DCPRequest req) {
            handleDCPRequest(req);
          }
        });
    System.out.println("Going to wait for " + waitTimeSec + "seconds ...");
    try {
      Thread.sleep(1000 * waitTimeSec);
    } catch (InterruptedException e) {
    }
    System.out.println("Wait over !!!");

    dcp.closedPartitions()
    .toBlocking()
    .forEach(new Action1<ClosedPartition>() {
      @Override
      public void call(ClosedPartition p) {
        changes.get(p.id()).state(p);
      }
    });

    s.unsubscribe();

    System.out.print(MathObservable.sumLong(dcp
        .stats()
        .map(new Func1<PartitionStats, Long>() {
          @Override
          public Long call(PartitionStats s) {
            return s.stats().get(PartitionStats.Stat.NUM_MUTATIONS);
          }
        }))
        .single()
        .toBlocking()
        .first()
        + " mutations with " + bytesCount + " bytes, in " + waitTimeSec + " seconds."
    );

    System.out.println("Closed partitions information.");
    for (Map.Entry<Short, PartitionChanges> change : changes.entrySet()) {
      System.out.println("Key: " + change.getKey() + ". Value: " + change.getValue().toString());
    }

    try {
      if (dataOutBackup != null) {
        dataOutBackup.close();
        dataOutBackup = null;
      }
  
      if (fOutBackup != null) {
        fOutBackup.close();
        fOutBackup = null;
      }
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private void handleDCPRequest(final DCPRequest req) {
    System.out.println("In handleDCPRequest");
    if (req instanceof MutationMessage) {
      MutationMessage msg = (MutationMessage) req;
      handleMutation(msg, msg.partition());
    } else if (req instanceof RemoveMessage) {
      RemoveMessage msg = (RemoveMessage) req;
      handleRemove(msg, msg.partition());
    }
  }

  private void handleMutation(final MutationMessage msg, short partition) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    ++mutationCount;
    System.out.print("\r" + mutationCount + "[" + (System.currentTimeMillis() - startTime) + "]");

    //System.out.println("Mutation key is  : " + msg.key());
    //System.out.println("Mutation len is  : " + msg.content().capacity());
    //System.out.println("Revision number  : " + msg.bySequenceNumber());
    bytesCount += msg.content().capacity();
    //if (msg.key().equals("FName2") || msg.key().equals("FName1"))
    //  System.out.println("Mutation value is: " + msg.content().toString(StandardCharsets.UTF_8));
/*
    try {
      dataOutBackup.writeUTF(msg.key());
      dataOutBackup.writeInt(msg.content().capacity());
      if (msg.content().capacity() != 0) {
        dataOutBackup.writeUTF(msg.content().toString(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      System.out.println(e);
    }
*/
  }

  private void handleRemove(final RemoveMessage msg, short partition) {
    //System.out.println("Remove key is  : " + msg.key());
  }
}
