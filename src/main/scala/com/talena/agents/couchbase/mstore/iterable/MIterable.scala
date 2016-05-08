package com.talena.agents.couchbase.mstore.iterable

import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileSystem}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/** A generic mechanism to iterate over partition groups given a list of Couchbase buckets.
  *
  * Decouples the traversal of partition groups from the operations that must be applied on them and
  * gives us flexibility into evolving more complex forms of traversals without changing the rest of
  * the code.
  *
  * Currently, a simple sequential iterable has been implemented, which enumerates all the partition
  * groups of all the buckets and visits them one by one in sequential order. We will add the
  * following iterables in the future:
  * - A parallel iterable that can visit multiple Couchbase buckets in parallel. This can be used
  *   when the number of Couchbase buckets is large.
  * - A parallel iterable that can visit multiple partition groups within a Couchbase bucket in
  *   parallel. This can be used when the number of Couchbase buckets is small, so that we can still
  *   exploit parallelism within each bucket.
  *
  * @param conf A reference to an active Spark configuration object.
  * @param bucketProps A list of [[com.talena.agents.couchbase.mstore.BucketProps]] objects over
  *                    which to iterate.
  */
abstract class MIterable(conf: SparkConf, bucketProps: List[BucketProps])
extends Iterable[PartitionGroupContext] with LazyLogging {
  /** Stops all active Spark Contexts */
  def stop()

  /** Creates an iterator for each bucket in bucketProps and stores them as a list of iterators */
  protected val buckets = bucketProps.map({ b =>
    val iter = Iterator.iterate((b.name, 1))({ case (_, p) => (b.name, p + 1) })
    iter.takeWhile(_._2 <= b.numPartitionGroups)
  })

  /** Convenience method for subclasses to create a new Env object.
    *
    * Subclasses are responsible for tracking the number of Env objets they create and cleaning them
    * up after they are done.
    */
  protected def newEnv(): Env = {
    val sparkCtx = new SparkContext(conf)
    val sqlCtx = new SQLContext(sparkCtx)
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val env = Env(conf, sparkCtx, sqlCtx, fs)
    logger.info(s"Created new Env: $env")

    env
  }
}

/** Companion object providing a factory method to instantiate a specific MIterable implementation
  * based on the Spark configuration object passed in.
  *
  * The SequentialIterable is the current default implementation that is chosen.
  */
object MIterable {
  def apply(conf: SparkConf, buckets: List[BucketProps]): MIterable = {
    MStoreProps.Iterable(conf) match {
      case "SequentialIterable" => new SequentialIterable(conf, buckets)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported iterable: " + unsupported)
    }
  }
}
