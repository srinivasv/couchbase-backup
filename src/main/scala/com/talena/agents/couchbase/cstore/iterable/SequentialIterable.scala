package com.talena.agents.couchbase.cstore.iterable

import com.talena.agents.couchbase.cstore.Bucket

import org.apache.spark.SparkConf

/** A simple sequential iterable over all partition groups of a list of buckets using a single
  * Spark context object.
  */
class SequentialIterable(conf: SparkConf, buckets: List[Bucket])
extends MIterable(conf, buckets) {
  /** Because this is a sequential iterable over all partition groups of all the buckets, we simply
    * concatenate the individual bucket iterators. Then for each (bucket, pgroup) pair in the
    * concatenated iterator, we build a [[com.talena.agents.couchbase.mstore.PartitionGroupContext]]
    * object by passing the only Env object that we have, and return that to the caller.
    */
  override def iterator(): Iterator[(String, String)] = {
    pgroups.tail
      .foldLeft(pgroups.head)((bucket1, bucket2) => bucket1 ++ bucket2)
      //.map({ case (bucket, pgid) => (bucket, pgid) })
  }
}
