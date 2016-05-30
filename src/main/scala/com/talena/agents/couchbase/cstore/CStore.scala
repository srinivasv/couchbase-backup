package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.cstore.iterable._
//import com.talena.agents.couchbase.cstore.manager._
import com.talena.agents.couchbase.cstore.pgroup._
import com.talena.agents.couchbase.cstore.pgroup.implicits._

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileSystem}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.reflect.ClassTag


/** Defines the properties of Couchbase buckets.
  *
  * @param name The bucket's name.
  * @param numPartitionGroups The number of partitionGroups in the bucket.
  */
case class Bucket(name: String, numPGroups: Integer)

/** An interface to support arbitrary transformations ("maps") over mutations.
  *
  * Callers of [[CStore.mapMutations]] should extend this class and provide an implementation of
  * the [[MutationsMapper.map]] method. Recovery, masking and sampling agents should use this class.
  */
abstract class MutationsMapper[A] {
  /** Called once before mapping begins */
  def setup(): Unit

  /** A callback that will do the actual transformation of the mutations.
    *
    * Specifically, the transformation will be from type
    * [[com.talena.agents.couchbase.core.CouchbaseLongRecord]] to type A. Internally, an iterator is
    * used to process the mutations. Concrete implementations of this interface are responsible for
    * batching mutations and other bookeeping tasks.
    *
    * @tparam A The output type of the transformation
    *
    * @param mTuple The next available mutation
    * @return A transformed mutation of type A
    */
  def map(mTuple: MutationTuple): A

  /** Called once after mapping ends */
  def teardown(): Unit
}

/** A static object encapsulating all the possible CStore operations.
  *
  * This is the entry point for all the external entities, such as the scheduler and the recovery,
  * masking and sampling agents.
  */
object CStore {
  def compactFilters(buckets: List[Bucket], from: String, to: String): Unit = {
    val env = newEnv(s"CStore filter compaction for $buckets")
    RunnableCStore.compactFilters(buckets, from, to)(env)
  }

  def compactMutations(buckets: List[Bucket], from: String, to: String): Unit = {
    val env = newEnv(s"CStore mutations compaction for $buckets")
    RunnableCStore.compactMutations(buckets, from, to)(env)
  }

  def moveCompactedMutations(buckets: List[Bucket], from: String, to: String): Unit = {
    val env = newEnv(s"CStore compacted mutations move for $buckets")
    RunnableCStore.moveCompactedMutations(buckets, from, to)(env)
  }

  def compactAll(buckets: List[Bucket], from: String, to: String): Unit = {
    val env = newEnv(s"CStore full compaction for $buckets")
    RunnableCStore.compactAll(buckets, from, to)(env)
  }

  def mapMutations[A: ClassTag](buckets: List[Bucket], from: String, mapper: MutationsMapper[A])
  : Unit = {
    val env = newEnv(s"CStore mutations mapping for $buckets")
    val runnable = RunnableCStore.mapMutations(buckets, from, mapper)
    runnable(env)
  }

  private def newEnv(appName: String): Env = {
    val conf = new SparkConf().setAppName(appName)
    val sparkCtx = new SparkContext(conf)
    val sqlCtx = new SQLContext(sparkCtx)
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    Env(conf, sparkCtx, sqlCtx, fs)
  }
}

object RunnableCStore extends LazyLogging {
  def compactFilters(buckets: List[Bucket], from: String, to: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactFilters() request")
      logger.info(s"buckets: $buckets, from: $from, to: $to")

      import twolevel.FilterCompactor._

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[Filters](bucket, pgid)
        logger.info(s"$pg: Compacting filter")
        compact(pg, from, to)(env)
      })

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[Filters](bucket, pgid)
        logger.info(s"($bucket, $pgid): Moving compacted filter")
        move(pg, to, from)(env)
      })

      logger.info(s"End compactFilters() request")
    })
  }

  def compactMutations(buckets: List[Bucket], from: String, to: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactMutations() request")
      logger.info(s"buckets: $buckets, snapshot: String, from: $from, to: $to")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[Mutations](bucket, pgid)
        logger.info(s"$pg: Compacting mutations")
        compact(pg, from, to)(env)
      })

      logger.info(s"End compactMutations() request")
    })
  }

  def moveCompactedMutations(buckets: List[Bucket], from: String, to: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin moveCompactMutations() request")
      logger.info(s"buckets: $buckets, from: $from, to: $to")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[Mutations](bucket, pgid)
        logger.info(s"$pg: Moving compacted mutations")
        move(pg, from, to)(env)
      })

      logger.info(s"End moveCompactedMutations() request")
    })
  }

  def compactAll(buckets: List[Bucket], from: String, to: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactAll() request")
      logger.info(s"buckets: $buckets, from: $from, to: $to")

      import twolevel.FullCompactor._

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[All](bucket, pgid)
        logger.debug(s"$pg: Compacting filters and mutations")
        compact(pg, from, to)(env)
      })

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[All](bucket, pgid)
        logger.debug(s"$pg: Moving compacted filters and mutations")
        move(pg, to, from)(env)
      })

      logger.info(s"End compactAll() request")
    })
  }

  /** "Maps" mutations from a snapshot using the given function (a callback) potentially
    * transforming it to something else.
    *
    * Will be used by the recovery, masking and sampling agents.
    *
    * @param repo UUID of the data repository
    * @param job ID of the job
    * @param props A list of [[Bucket]] objects describing the buckets involved
    * @param snapshot The specific snapshot to use for reading mutations
    * @param mapper An instance of an[[com.talena.agents.couchbase.mstore.MutationsMapper]] derived
    *               class that will do the actual transformation of the mutations.
    * @return The transformed mutations as a [[MappedMutations]] object of type A
    */
  def mapMutations[A: ClassTag](buckets: List[Bucket], from: String, mapper: MutationsMapper[A])
  : Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin mapMutations() request")
      logger.info(s"buckets: $buckets, from: $from")

      import twolevel.CompactedMutationsReader._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[Mutations](bucket, pgid)
        val runnableMappedRdd = for {
          rdd <- read(pg, from)
          mappedRDD = rdd.mapPartitions[A]({ partition =>
              mapper.setup()
              val mappedPartition = for {
                mutation <- partition
              } yield mapper.map(mutation)
              mapper.teardown()

              mappedPartition
            }, preservesPartitioning = true)
        } yield mappedRDD

        runnableMappedRdd(env).collect()
      })

      //res.map(r => { logger.info("Before collect"); r.collect() })
      logger.info(s"End mapMutations() request")
    })
  }
}
