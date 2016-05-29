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
case class BucketProps(name: String, numPartitionGroups: Integer)

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
  def compactFilters(repo: String, job: String, props: List[BucketProps], temp: String): Unit = {
    val env = newEnv(s"CStore filter compaction for job $job")
    RunnableCStore.compactFilters(repo, job, props, temp)(env)
  }

  def compactMutations(repo: String, job: String, props: List[BucketProps], snapshot: String,
      temp: String): Unit = {
    val env = newEnv(s"CStore mutations compaction of snapshot $snapshot for job $job")
    RunnableCStore.compactMutations(repo, job, props, snapshot, temp)(env)
  }

  def moveCompactedMutations(repo: String, job: String, props: List[BucketProps], temp: String)
  : Unit = {
    val env = newEnv(s"CStore compacted mutations move for job $job")
    RunnableCStore.moveCompactedMutations(repo, job, props, temp)(env)
  }

  def compactAll(repo: String, job: String, props: List[BucketProps], temp: String): Unit = {
    val env = newEnv(s"CStore full compaction for job $job")
    RunnableCStore.compactAll(repo, job, props, temp)(env)
  }

  def mapMutations[A: ClassTag](repo: String, job: String, props: List[BucketProps],
      snapshot: String, mapper: MutationsMapper[A]): Unit = {
    val env = newEnv(s"CStore mutations mapping of snapshot $snapshot for job $job")
    val runnable = RunnableCStore.mapMutations(repo, job, props, snapshot, mapper)
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
  def compactFilters(repo: String, job: String, props: List[BucketProps], temp: String)
  : Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactFilters() request")
      logger.info(s"repo: $repo, job: $job, props: $props, temp: $temp")

      import twolevel.FilterCompactor._

      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Compacting filter")
        compact(PGroup[Filters](repo, job, bucket, pgid), Mainline(), temp)(env)
      })

      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Moving compacted filter")
        move(PGroup[Filters](repo, job, bucket, pgid), temp)(env)
        logger.info(s"($bucket, $pgid): Cleaning up")
        cleanup(PGroup[Filters](repo, job, bucket, pgid))(env)
      })

      logger.info(s"End compactFilters() request")
    })
  }

  def compactMutations(repo: String, job: String, props: List[BucketProps], snapshot: String,
      temp: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactMutations() request")
      logger.info(s"repo: $repo, job: $job, props: $props, snapshot: $snapshot, temp: $temp")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Compacting mutations")
        compact(PGroup[Mutations](repo, job, bucket, pgid), Snapshot(snapshot), temp)(env)
      })

      logger.info(s"End compactMutations() request")
    })
  }

  def moveCompactedMutations(repo: String, job: String, props: List[BucketProps], temp: String)
  : Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin moveCompactMutations() request")
      logger.info(s"repo: $repo, job: $job, props: $props, temp: $temp")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Moving compacted mutations")
        move(PGroup[Mutations](repo, job, bucket, pgid), temp)(env)
        logger.info(s"($bucket, $pgid): Cleaning up")
        cleanup(PGroup[Mutations](repo, job, bucket, pgid))(env)
      })

      logger.info(s"End moveCompactedMutations() request")
    })
  }

  def compactAll(repo: String, job: String, props: List[BucketProps], temp: String)
  : Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactAll() request")
      logger.info(s"repo: $repo, job: $job, props: $props, temp: $temp")

      import twolevel.FullCompactor._

      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Compacting filters and mutations")
        compact(PGroup[All](repo, job, bucket, pgid), Mainline(), temp)(env)
      })

      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        logger.info(s"($bucket, $pgid): Moving compacted filters and mutations")
        move(PGroup[All](repo, job, bucket, pgid), temp)(env)
        logger.info(s"($bucket, $pgid): Cleaning up")
        cleanup(PGroup[All](repo, job, bucket, pgid))(env)
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
    * @param props A list of [[BucketProps]] objects describing the buckets involved
    * @param snapshot The specific snapshot to use for reading mutations
    * @param mapper An instance of an[[com.talena.agents.couchbase.mstore.MutationsMapper]] derived
    *               class that will do the actual transformation of the mutations.
    * @return The transformed mutations as a [[MappedMutations]] object of type A
    */
  def mapMutations[A: ClassTag](repo: String, job: String, props: List[BucketProps],
      snapshot: String, mapper: MutationsMapper[A]): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin mapMutations() request")
      logger.info(s"repo: $repo, job: $job, props: $props, snapshot: $snapshot")

      import twolevel.CompactedMutationsReader._
      MIterable(env.conf, props).map({ case (bucket, pgid) =>
        val pg = PGroup[Mutations](repo, job, bucket, pgid)
        val runnable = for {
          rdd <- read(pg, Snapshot(snapshot))
          mappedRDD = rdd.mapPartitions[A]({ partition =>
              mapper.setup()

              val mappedPartition = for {
                mutation <- partition
              } yield mapper.map(mutation)

              mapper.teardown()
              mappedPartition
            }, preservesPartitioning = true)
        } yield mappedRDD

        runnable(env)
      })

      logger.info(s"End mapMutations() request")
    })
  }
}
