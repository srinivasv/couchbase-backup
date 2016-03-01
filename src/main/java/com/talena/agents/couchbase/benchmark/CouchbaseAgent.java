package com.talena.agents.couchbase.benchmark;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.benchmark.BenchmarkConfig.OP_TYPE;
import com.talena.agents.couchbase.core.ClosedPartition;
import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.DCPEndpoint;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.PartitionStats;

import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.MathObservable;

public class CouchbaseAgent {
  private BenchmarkConfig config;
  private CouchbaseFacade cbFacade;
  private CouchbaseCatalog cbCatalog;
  private long count;

  public CouchbaseAgent(BenchmarkConfig config) {
    this.config = config;
    this.cbCatalog = new CouchbaseCatalog("/home/navendu/cb/cb.catalog");

    this.cbFacade = new CouchbaseFacade(this.config.getNodesIp(),
        this.config.getBucketName(), "");
  }

  public void backup() {
    cbCatalog.load();
    System.out.println("Loaded into catalog " + cbCatalog.getCatalog().size() + " entries.");

    cbFacade.openBucket();
    Map<Short, Long> highSeqNos = cbFacade.currentHighSeqnos();

    DCPEndpoint dcp = cbFacade.dcpEndpoint();

    dcp = cbFacade.dcpEndpoint();
    dcp.openConnection("streamConn", new HashMap<String, String>());

    Map<Short, PartitionSpec> specs = getChangePartitionsSpec(highSeqNos);

    if (specs.isEmpty()) {
      System.out.println("Nothing to backup.");

      return;
    }

    long startTime = System.currentTimeMillis();
    count = 0;

    System.out.println("Starting streams ...");
    dcp.streamPartitions(specs)
      .toBlocking()
      .forEach(new Action1<DCPRequest>() {
        @Override
        public void call(DCPRequest dcpRequest) {
          handleDCPRequest(dcpRequest);
        }
      });

    long endTime = System.currentTimeMillis();

    System.out.println("Done backingup data.");

    long mutationsCount = MathObservable.sumLong(dcp
        .stats()
        .map(new Func1<PartitionStats, Long>() {
          @Override
          public Long call(PartitionStats s) {
            return s.stats().get(PartitionStats.Stat.NUM_MUTATIONS);
          }
        }))
        .single()
        .toBlocking()
        .first();

    dcp.closedPartitions()
      .toBlocking()
      .forEach(new Action1<ClosedPartition>() {
        @Override
        public void call(ClosedPartition closedPart) {
          CatalogData data = new CatalogData();

          data.setvBucketUuid(closedPart.failoverLog().get(0).vbucketUUID());
          data.setLastSeqNo(closedPart.lastSeenSeqno());

          cbCatalog.getCatalog().put(closedPart.id(), data);

          System.out.println("Closed Partition {"
              + "idPartition = " + closedPart.id()
              + ", last seen seq no = " + closedPart.lastSeenSeqno()
              + ", vBucket UUID = " + closedPart.failoverLog().get(0).vbucketUUID()
              + ", last snapshort start seq no = " + closedPart.lastSnapshotStartSeqno()
              + ", last snapshort end seq no = " + closedPart.lastSnapshotEndSeqno()
              + "}");
        }
      });

    System.out.println("Saving to catalog " + cbCatalog.getCatalog().size() + " entries.");
    cbCatalog.store();

    System.out.println(mutationsCount + " mutations, in " + (endTime - startTime) + " ms.");
  }

  private Map<Short, PartitionSpec> getChangePartitionsSpec(Map<Short, Long> highSeqNos) {
    Map<Short, PartitionSpec> specs = new HashMap<Short, PartitionSpec>();
    for (short idPartition : highSeqNos.keySet()) {
      if (highSeqNos.get(idPartition) == 0) {
        continue;
      }

      long vBucketUuid = 0;
      long startSeqNo = 0;
      CatalogData data = cbCatalog.getCatalog().get(idPartition);

      if (data != null) {
        startSeqNo = data.getLastSeqNo();
        vBucketUuid = data.getvBucketUuid();
      }

      PartitionSpec spec;

      if (config.getOperation() == OP_TYPE.BACKUP_INCR) {
        if (startSeqNo == highSeqNos.get(idPartition)) {
          continue;
        }

        spec = new PartitionSpec(idPartition, vBucketUuid, startSeqNo, highSeqNos.get(idPartition), startSeqNo, highSeqNos.get(idPartition));
      } else {
        spec = new PartitionSpec(idPartition, 0, 0, highSeqNos.get(idPartition), 0, 0);
      }
  
      specs.put(idPartition, spec);
    }

    return specs;
  }

  private void handleDCPRequest(final DCPRequest dcpRequest) {
    if (dcpRequest instanceof MutationMessage) {
      MutationMessage msg = (MutationMessage) dcpRequest;

      handleMutation(msg, msg.partition());
    } else if (dcpRequest instanceof RemoveMessage) {
      RemoveMessage msg = (RemoveMessage) dcpRequest;

      handleRemove(msg, msg.partition());
    }
  }

  private void handleMutation(final MutationMessage msg, short idPartition) {
    //System.out.println("Mutated Key: " + msg.key() + "\nContent: " + msg.content().toString(StandardCharsets.UTF_8));
    ++count;
    System.out.print("\rGot #" + count);
  }

  private void handleRemove(final RemoveMessage msg, short idPartition) {
    //System.out.println("Removed Key: " + msg.key());
  }
}
