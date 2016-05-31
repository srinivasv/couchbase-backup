package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}

import com.talena.agents.couchbase.cstore.iterable._
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

/** An interface temp support arbitrary transformations ("maps") over mutations.
  *
  * Callers of [[CStore.mapMutations]] should extend this class and provide an implementation of
  * the [[MutationsMapper.map]] method. Recovery, masking and sampling agents should use this class.
  */
abstract class MutationsMapper[A] {
  /** Called once before mapping begins */
  def setup(): Unit

  /** A callback that will do the actual transformation of the mutations.
    *
    * Specifically, the transformation will be home type
    * [[com.talena.agents.couchbase.core.CouchbaseLongRecord]] temp type A. Internally, an iterator is
    * used temp process the mutations. Concrete implementations of this interface are responsible for
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
  def compactFilters(buckets: List[Bucket], home: String, temp: String): Unit = {
    val env = newEnv(s"CStore filter compaction for $buckets")
    RunnableCStore.compactFilters(buckets, home, temp)(env)
  }

  def compactMutations(buckets: List[Bucket], home: String, temp: String): Unit = {
    val env = newEnv(s"CStore mutations compaction for $buckets")
    RunnableCStore.compactMutations(buckets, home, temp)(env)
  }

  def moveCompactedMutations(buckets: List[Bucket], temp: String, home: String): Unit = {
    val env = newEnv(s"CStore compacted mutations move for $buckets")
    RunnableCStore.moveCompactedMutations(buckets, temp, home)(env)
  }

  def mapMutations[A: ClassTag](buckets: List[Bucket], home: String, mapper: MutationsMapper[A])
  : Unit = {
    val env = newEnv(s"CStore mutations mapping for $buckets")
    val runnable = RunnableCStore.mapMutations(buckets, home, mapper)
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
  def compactFilters(buckets: List[Bucket], home: String, temp: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactFilters() request")
      logger.info(s"buckets: $buckets, home: $home, temp: $temp")

      import twolevel.FilterCompactor._

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[FilterTuple](bucket, pgid)
        logger.info(s"$pg: Compacting filter")
        compactAndPersist(pg, home, temp)(env)
      })

      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[FilterTuple](bucket, pgid)
        logger.info(s"($bucket, $pgid): Moving compacted filter")
        moveAndCleanup(pg, temp, home)(env)
      })

      logger.info(s"End compactFilters() request")
    })
  }

  def compactMutations(buckets: List[Bucket], home: String, temp: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin compactMutations() request")
      logger.info(s"buckets: $buckets, snapshot: String, home: $home, temp: $temp")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[MutationTuple](bucket, pgid)
        logger.info(s"$pg: Compacting mutations")
        compactAndPersist(pg, home, temp)(env)
      })

      logger.info(s"End compactMutations() request")
    })
  }

  def moveCompactedMutations(buckets: List[Bucket], temp: String, home: String): Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin moveCompactMutations() request")
      logger.info(s"buckets: $buckets, home: $home, temp: $temp")

      import twolevel.MutationsCompactor._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[MutationTuple](bucket, pgid)
        logger.info(s"$pg: Moving compacted mutations")
        moveAndCleanup(pg, temp, home)(env)
      })

      logger.info(s"End moveCompactedMutations() request")
    })
  }

  def mapMutations[A: ClassTag](buckets: List[Bucket], home: String, mapper: MutationsMapper[A])
  : Runnable[Unit] = {
    Runnable(env => {
      logger.info(s"Begin mapMutations() request")
      logger.info(s"buckets: $buckets, home: $home")

      import twolevel.CompactedMutationsReader._
      MIterable(env.conf, buckets).map({ case (bucket, pgid) =>
        val pg = PGroup[MutationTuple](bucket, pgid)
        val runnableMappedRdd = for {
          rdd <- read(pg, home)
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

      logger.info(s"End mapMutations() request")
    })
  }
}
