package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore.iterable._
import com.talena.agents.couchbase.mstore.manager._

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileSystem}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.reflect.ClassTag

/** A static object encapsulating all the possible MStore operations.
  *
  * This is the entry point for all the external entities, such as the scheduler and the recovery,
  * masking and sampling agents.
  */
object MStore extends LazyLogging {
  /** Adds new buckets to the storage hierarchy.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets being added
    */
  def addBuckets(dataRepo: String, job: String, bProps: List[BucketProps]) = {
    logger.info(s"Begin addBuckets() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      manager.addPartitionGroup(pgCtx)
    })

    logger.info(s"End addBuckets() request")
  }

  /** Removes existing buckets from the storage hierarchy.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets being removed
    */
  def removeBuckets(dataRepo: String, job: String, bProps: List[BucketProps]) = {
    logger.info(s"Begin removeBuckets() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      manager.removePartitionGroup(pgCtx)
    })

    logger.info(s"End removeBuckets() request")
  }

  /** Compacts both the filters and mutations off the mainline, compacting the mutations after the
    * filters have been compacted.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets involved
    * @param tmpLoc A temporary location where the compacted filter will be written to. A subsequent
    * move operation will move the compacted filter and mutations from the temporary to their final
    * location.
    */
  def compact(dataRepo: String, job: String, bProps: List[BucketProps], tmpLoc: String) = {
    logger.info(s"Begin compact() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps, tmpLoc: $tmpLoc")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Compacting mutations for $pgCtx")
      val compactedFilters = manager.compactFilters(pgCtx)

      // Use the InlineWithFiltersCompaction mode of filtering the mutations (see
      // [[com.talena.agents.couchbase.mstore.PartitionGroupManager.MutationsFilteringMode]])
      val compactedMutations = manager.compactMutations(pgCtx, InlineWithFiltersCompaction(
        compactedFilters))

      manager.persistCompactedFilters(pgCtx, compactedFilters, tmpLoc)

      // compactedMutations is an Option[Map[String, MappedMutations[MutationTuple]]]. Here, we
      // invoke Option's map() method to extract the actual value (if it is present) and persist it.
      // If Option is empty (None), it means there was nothing to compact.
      compactedMutations
        .map(m => manager.persistCompactedMutations(pgCtx, m, tmpLoc))
        .getOrElse(logger.info(s"No mutations to compact for $pgCtx"))
    })

    /** Move the compacted filters and mutations to their final location */
    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Moving compacted mutations for $pgCtx to the mainline")
      manager.moveCompactedFilters(pgCtx, tmpLoc)
      manager.moveCompactedMutations(pgCtx, tmpLoc)
    })

    logger.info(s"End compact() request")
  }

  /** Compacts one or more buckets' filters off the mainline.
    *
    * This method may be invoked on its own allowing us to decouple the filter and mutations
    * compaction operations. Thus, mutations compaction may be run as an offline process if desired.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets involved
    * @param tmpLoc A temporary location where the compacted filter will be written to. A subsequent
    * move operation will move the compacted filter from the temporary to its final location.
    */
  def compactFilters(dataRepo: String, job: String, bProps: List[BucketProps], tmpLoc: String) = {
    logger.info(s"Begin compactFilters() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps, tmpLoc: $tmpLoc")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Compacting filters for $pgCtx")
      val compactedFilters = manager.compactFilters(pgCtx)
      manager.persistCompactedFilters(pgCtx, compactedFilters, tmpLoc)
    })

    /** Move the compacted filters and the uncompacted mutations to their final location */
    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Moving compacted mutations for $pgCtx to the mainline")
      manager.moveCompactedFilters(pgCtx, tmpLoc)
      manager.moveUncompactedMutations(pgCtx)
    })

    logger.info(s"End compactFilters() request")
  }

  /** Compacts one or more buckets' mutations off a given snapshot if the compaction criteria is
    * met.
    *
    * This method may be invoked in an offline mode so it could run as a background process.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets involved
    * @param snapshot The specific snapshot to use for reading mutations
    * @param tmpLoc A temporary location where the compacted mutations will be written to. A
    * subsequent move operation will move the compacted mutations from the temporary to their final
    * location.
    */
  def compactMutations(dataRepo: String, job: String, bProps: List[BucketProps], snapshot: String,
      tmpLoc: String) = {
    logger.info(s"Begin compactMutations() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps, snapshot: $snapshot, " +
      "tmpLoc: $tmpLoc")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Compacting mutations for $pgCtx in snapshot $snapshot")

      // Use the Offline mode (based off a snapshot) of filtering the mutations (see
      // [[com.talena.agents.couchbase.mstore.PartitionGroupManager.MutationsFilteringMode]])
      val compactedMutations = manager.compactMutations(pgCtx, Offline(snapshot))

      // compactedMutations is an Option[Map[String, MappedMutations[MutationTuple]]]. Here, we
      // invoke Option's map() method to extract the actual value (if it is present) and persist it.
      // If Option is empty (None), it means there was nothing to compact.
      compactedMutations
        .map(m => manager.persistCompactedMutations(pgCtx, m, tmpLoc))
        .getOrElse(logger.info(s"No mutations to compact for $pgCtx in snapshot $snapshot"))
    })

    logger.info(s"End compactMutations() request")
  }

  /** Moves one or more buckets' compacted mutations generated by a previous call to
    * compactMutations() from the temp location specified then to the mainline.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets involved
    * @param tmpLoc The temporary location that was specified in the call to compactMutations()
    * where the compacted mutations are now present.
    */
  def moveCompactedMutations(dataRepo: String, job: String, bProps: List[BucketProps],
      tmpLoc: String) = {
    logger.info(s"Begin moveCompactedMutations() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps, tmpLoc: $tmpLoc")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Moving compacted mutations for $pgCtx to the mainline")
      manager.moveCompactedMutations(pgCtx, tmpLoc)
    })

    logger.info(s"End moveCompactedMutations() request")
  }

  /** "Maps" mutations from a snapshot using the given function (a callback) potentially
    * transforming it to something else.
    *
    * Will be used by the recovery, masking and sampling agents.
    *
    * @param dataRepo UUID of the data repository
    * @param job ID of the job
    * @param bProps A list of [[BucketProps]] objects describing the buckets involved
    * @param snapshot The specific snapshot to use for reading mutations
    * @param mapper An instance of an[[com.talena.agents.couchbase.mstore.MutationsMapper]] derived
    *               class that will do the actual transformation of the mutations.
    * @return The transformed mutations as a [[MappedMutations]] object of type A
    */
  def mapMutations[A: ClassTag](dataRepo: String, job: String, bProps: List[BucketProps],
      snapshot: String, mapper: MutationsMapper[A]) = {
    logger.info(s"Begin mapMutations() request")
    logger.info(s"dataRepo: $dataRepo, job: $job, bProps: $bProps, snapshot: $snapshot")

    using(dataRepo, job, bProps)((manager, pgCtx) => {
      logger.info(s"Mapping mutations for $pgCtx")
      mapper.setup()
      manager.mapMutations[A](pgCtx, Offline(snapshot), m => mapper.map(m))
      mapper.teardown()
    })

    logger.info(s"End mapMutations() request")
  }

  /** A helper method to abstract out the common functionality from the public APIs of this class.
    *
    * Does the following:
    * - Creates a new Spark configuration object.
    * - Picks a suitable iterable based on the settings passed in the configuration object.
    * - Similarly, picks an instance of PartitionGroupManager.
    * - Invokes the iterable, and for each PartitionGroupContext object passed in by the iterable,
    *   it invokes the provided function "f" with the following parameters:
    *   - A reference to the manager object initialized above.
    *   - The PartitionGroupContext object passed in by the iterable.
    *
    * This allows the callers of this helper method to simply pass in a function that's specific to
    * their API and delegate all other tasks (described above) to this method.
    *
    * We use function currying here. There are two sets of parameters that this method expects:
    * - The first is the set of dataRepo, job and bProps values.
    * - The second is the function that needs to be invoked.
    *
    * Using currying here makes sense because at each of the call sites the code becomes easy and
    * intuitive to read. For example, `using(dataRepo, job, bProps)(doSomething...)` can be read as
    * "using the given parameters, do something".
    */
  private def using(dataRepo: String, job: String, bProps: List[BucketProps])(
      f: (PartitionGroupManager, PartitionGroupContext) => Unit) = {
    logger.info("Initializing MStore")
    val conf = new SparkConf()
    val iterable = MIterable(conf, bProps)
    val manager = PartitionGroupManager(conf, dataRepo, job)
    logger.info(s"conf: $conf, iterable: $iterable, manager: $manager")

    iterable.map(pgCtx => f(manager, pgCtx))
    iterable.stop()
  }
}

/** Defines the properties of Couchbase buckets.
  *
  * @param name The bucket's name.
  * @param numPartitionGroups The number of partitionGroups in the bucket.
  */
case class BucketProps(name: String, numPartitionGroups: Integer)

/** A generic placeholder for a bunch of environmental object references.
  *
  * @param sparkCtx Reference to an active Spark context object.
  * @param sqlCtx Reference to an active SparkSQL context object.
  * @param fs Reference to a filesystem object.
  */
case class Env(conf: SparkConf, sparkCtx: SparkContext, sqlCtx: SQLContext, fs: FileSystem)

/** Defines what a partition group is.
  *
  * @param id An ID for the partition group.
  * @param bucket The Couchbase bucket to which the partition group belongs.
  * @param env A reference to the [[Env]] object assigned to this partition group.
  */
case class PartitionGroupContext(id: String, bucket: String, env: Env)

/** An interface to support arbitrary transformations ("maps") over mutations.
  *
  * Callers of [[MStore.mapMutations]] should extend this class and provide an implementation of
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
