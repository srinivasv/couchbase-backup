package com.talena.agents.couchbase.benchmark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.PartitionChanges;
import com.talena.agents.couchbase.core.ClosedPartition;
import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.DCPEndpoint;
import com.talena.agents.couchbase.core.OpenPartition;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.PartitionStats;

import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.MathObservable;

public class BenchmarkBackup {
  private long bytesCount = 0;
  private long startTime = 0;
  private FileChannel fcOut = null;
  private Map<Short, PartitionChanges> changes;
  private long mutationCount = 0;

  public static void main(String[] args) {
    if (args.length < 4) {
      System.out.println("Usage: <bucket> <backup-path> <Full/Incremental> <ipNodes>");

      return;
    }

    String bucket = args[0];
    String backupPath = args[1];
    String backupType = args[2];
    String nodes[] = new String[args.length - 3];
    boolean isFullBackup = true;

    System.arraycopy(args, 3, nodes, 0, args.length - 3);

    if (backupType.compareToIgnoreCase("Full") == 0) {
      System.out.print("Taking full backup of ");
    } else if (backupType.compareToIgnoreCase("Incremental") == 0) {
      isFullBackup = false;
      System.out.print("Taking incremental backup of ");
    } else {
      System.out.println("Usage: <bucket> <backup-path> <Full/Incremental> <ipNodes>");

      return;
    }

    System.out.println("bucket: " + bucket + ", at " + backupPath);
    System.out.print("Connecting to nodes: ");
    for (String node : nodes) {
      System.out.print(node + " ");
    }
    System.out.println("");

    BenchmarkBackup app = new BenchmarkBackup();

    app.Run(bucket, nodes, backupPath, isFullBackup);
  }

  public void Run(String bucket, String[] nodes, String backupPath, boolean isFullBackup) {
    CouchbaseFacade cbFacade = new CouchbaseFacade(nodes, bucket, "");
    cbFacade.openBucket();

    Map<Short, Long> highSeqNos = cbFacade.currentHighSeqnos();
    short changedPartitions[] = new short[1024];
    int changedPartitionsCount = 0;

    System.out.println("vBuckets high sequence numbers are ...");

    File metaFile = new File(backupPath + "meta.data");
    File backupFile = new File(backupPath + "backup.data" + (isFullBackup ? ".full" : ".incr"));
    Map<Short, Long> metaData = new HashMap<Short, Long>();

    if (!metaFile.exists()) {
      System.out.println(metaFile.getAbsolutePath() + " metafile does not exist. Can only do full backup.");
      isFullBackup = true;
    }

    for (Map.Entry<Short, Long> highSeqNo : highSeqNos.entrySet()) {
      if (highSeqNo.getValue() != 0) {
        System.out.println(highSeqNo.getKey() + " " + highSeqNo.getValue());
        changedPartitions[changedPartitionsCount] = highSeqNo.getKey().shortValue();
        ++changedPartitionsCount;
      }
    }

    try {
      fcOut = new FileOutputStream(backupFile).getChannel();
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
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
        PartitionSpec partSpec = new PartitionSpec(idChanged, 0, startSeqNo, endSeqNo, 0, 0);
        System.out.println("For partition " + idChanged + " range is " + startSeqNo + " to " + endSeqNo);
  
        specs.put(idChanged, partSpec);
      }
    }

    changes = new HashMap<Short, PartitionChanges>(specs.size());
    for (PartitionSpec s : specs.values()) {
      short id = s.id();
      changes.put(id, new PartitionChanges());
    }

    for (int i = 3; i >= 1; --i) {
      System.out.print("\rStarting in " + i + " seconds.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    System.out.println("");

    long startTime = System.currentTimeMillis();

    dcp.streamPartitions(specs)
    .toBlocking()
    .forEach(new Action1<DCPRequest>() {
      public void call(DCPRequest dcpRequest) {
        handleDCPRequest(dcpRequest);
      }
    });

    dcp.closedPartitions()
    .toBlocking()
    .forEach(new Action1<ClosedPartition>() {
      public void call(ClosedPartition p) {
        changes.get(p.id()).state(p);
      }
    });

    dcp.openPartitions()
    .toBlocking()
    .forEach(new Action1<OpenPartition>() {
      public void call(OpenPartition p) {
      }
    });

    System.out.println("All mutations received.");

    long endTime = System.currentTimeMillis();

    System.out.print(MathObservable.sumLong(dcp
        .stats()
        .map(new Func1<PartitionStats, Long>() {
          public Long call(PartitionStats s) {
            return s.stats().get(PartitionStats.Stat.NUM_MUTATIONS);
          }
        }))
        .single()
        .toBlocking()
        .first()
        + " mutations with " + bytesCount + " bytes, in " + (endTime - startTime) + " milliseconds."
    );

    try {
      if (fcOut != null) {
        fcOut.close();
      }
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private void handleDCPRequest(final DCPRequest req) {
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

    try {
      msg.content().getBytes(0, fcOut, msg.content().capacity());
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.print("\rWritten mutation # " + mutationCount);

    bytesCount += msg.content().capacity();
    msg.content().release();
    msg.connection().consumed(msg);
  }

  private void handleRemove(final RemoveMessage msg, short partition) {
  }
}
