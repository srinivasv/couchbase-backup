package com.talena.agents.couchbase.mstore.iterable

import com.talena.agents.couchbase.mstore.{BucketProps, Env, PartitionGroupContext}

import org.apache.spark.SparkConf

/** A simple sequential iterable over all partition groups of a list of buckets using a single
  * Spark context object.
  */
class SequentialIterable(conf: SparkConf, bucketProps: List[BucketProps])
extends MIterable(conf, bucketProps) {
  /** Because this is a sequential iterable over all partition groups of all the buckets, we simply
    * concatenate the individual bucket iterators. Then for each (bucket, pgroup) pair in the
    * concatenated iterator, we build a [[com.talena.agents.couchbase.mstore.PartitionGroupContext]]
    * object by passing the only Env object that we have, and return that to the caller.
    */
  override def iterator(): Iterator[PartitionGroupContext] = {
    buckets.tail
      .foldLeft(buckets.head)((bucket1, bucket2) => bucket1 ++ bucket2)
      .map({ case (bucket, pgroup) =>
        PartitionGroupContext(pgroup.toString, bucket, env)
      })
  }

  override def stop() = env.sparkCtx.stop()

  /** The sole Env object we use in this iterable */
  val env: Env = newEnv()
}
