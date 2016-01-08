package com.talena.agents.couchbase.core;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensResponse;
import com.couchbase.client.core.message.kv.MutationToken;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * <p>A wrapper for the essential Couchbase server operations required from a
 * backup and restore perspective.<p>
 */
public class CouchbaseFacade {
  private final static Logger logger = Logger.getLogger(CouchbaseFacade.class);
  private final static int CONNECT_TIMEOUT = 5;

  // Essential Couchbase cluster parameters with which we will be interacting
  private final String[] nodes;
  private final String bucket;
  private final String password;

  private final ClusterFacade core;

  public CouchbaseFacade(final String[] nodes, final String bucket,
    final String password) {

    this.nodes = nodes;
    this.bucket = bucket;
    this.password = password;

    this.core = new CouchbaseCore(DefaultCoreEnvironment
      .builder()
      .dcpEnabled(true)
      .mutationTokensEnabled(true)
      .build());
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

  public void openBucket() {
    logger.info(String.format("Sending seed nodes request: %s", nodes));
    core.send(new SeedNodesRequest(nodes))
      .timeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
      .toBlocking()
      .single();

    logger.info(String.format("Opening bucket: %s", bucket));
    core.send(new OpenBucketRequest(bucket, password))
      .timeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
      .toBlocking()
      .single();
  }

  /**
   * Fetches the current values of the high seqnos of all the partitions in the
   * Couchbase cluster.
   */
  public Map<Short, Long> currentHighSeqnos() {
    logger.info(String.format("Fetching current high seqnos"));
    Map<Short, Long> seqnos = new HashMap<Short, Long>();
    MutationToken[] tokens = core.<GetAllMutationTokensResponse>send(
      new GetAllMutationTokensRequest(
        GetAllMutationTokensRequest.PartitionState.ACTIVE, bucket))
      .single().toBlocking().first().mutationTokens();
    for (MutationToken token : tokens) {
      seqnos.put((short) token.vbucketID(), token.sequenceNumber());
    }
    logger.info(String.format("Current high seqnos: %s", seqnos));
    return seqnos;
  }

  /**
   * Fetches the number of partitions in the Couchbase cluster.
   */
  public int numPartitions() {
    return currentHighSeqnos().size();
  }

  /**
   * Creates a new DCP endpoint for the given bucket.
   */
  public DCPEndpoint dcpEndpoint() {
    return new DCPEndpoint(core, bucket);
  }
}
