package com.talena.agents.couchbase.mstore.manager

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.SparkConf

import scala.reflect.ClassTag

/** A generic partition group manager interface encapsulating all of the possible operations on
  * individual partition groups.
  *
  * This interface exposes an abstract notion of a partition group. It is up to the subclasses to
  * decide the actual physical layout of a partition group. A two level partition group manager
  * may organize partition groups in two levels L0 and L1. Similarly, a three level manager may do
  * the same across three levels L0, L1 and L2.
  *
  * Because the physical layout of partition groups is left to the subclasses, the return types of
  * some of the methods of this trait are maps over the actual returned objects. This is true of
  * methods that return the [[com.talena.agents.couchbase.mstore.CompactedFilter]] and
  * [[com.talena.agents.couchbase.mstore.MappedMutations]] objects. The reason is that subclasses
  * may return a multitude of these objects corresponding to the different levels they support.
  * Using a map lets them tag the objects by the level to which they belong so that they may be
  * looked up later if required.
  *
  * Overall, this makes it possible to define a generic API between the callers and the partition
  * manager implementations, with neither the caller nor the PartitionGroupManager interface aware
  * of the actual physical layout of partition groups.
  *
  * @param conf A reference to an active Spark configuration object.
  * @param dataRepo Data repository UUID
  * @param job Job ID
  */
abstract class PartitionGroupManager(conf: SparkConf, dataRepo: String, job: String)
extends LazyLogging {
  /** Adds a new partition group to the storage hierarchy.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group being added.
    */
  def addPartitionGroup(ctx: PartitionGroupContext): Unit

  /** Removes an existing partition group from the storage hierarchy.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group being removed.
    */
  def removePartitionGroup(ctx: PartitionGroupContext): Unit

  /** Compacts all applicable filters of a partition group.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group to target.
    * @return A map of new [[com.talena.agents.couchbase.mstore.CompactedFilter]] objects.
    */
  def compactFilters(ctx: PartitionGroupContext): Map[String, CompactedFilter]

  /** Saves all compacted filters of a partition group.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group to target.
    * @param filters The map of [[com.talena.agents.couchbase.mstore.CompactedFilter]] objects
    *                returned in a previous call to compactFilters().
    * @param dest The location where the filters must be saved.
    */
  def persistCompactedFilters(ctx: PartitionGroupContext, filters: Map[String, CompactedFilter],
      dest: String): Unit

  /** Compacts all applicable mutations of a partition group.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group to target.
    * @param mode The mode specifying how the mutations should be filtered.
    *             See [[MutationsFilteringMode]].
    * @return A map of new [[com.talena.agents.couchbase.mstore.MappedMutations]] objects.
    */
  def compactMutations(ctx: PartitionGroupContext, mode: MutationsFilteringMode)
  : Option[Map[String, MappedMutations[MutationTuple]]]

  /** Saves all compacted mutations of a partition group.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group to target.
    * @param mutations The map of [[com.talena.agents.couchbase.mstore.MappedMutations]] objects
    *                returned in a previous call to compactMutations().
    * @param dest The location where the mutations must be saved.
    */
  def persistCompactedMutations(ctx: PartitionGroupContext,
      mutations: Map[String, MappedMutations[MutationTuple]], dest: String): Unit

  /** Transforms ALL mutations of a partition group using a function.
    *
    * @param ctx A [[com.talena.agents.couchbase.mstore.PartitionGroupContext]] object specifying
    *            the partition group to target.
    * @param mode The mode specifying how the mutations should be filtered.
    *             See [[MutationsFilteringMode]].
    * @param mappingFunc The function to use to transformm the mutations.
    * @return A map of new [[com.talena.agents.couchbase.mstore.MappedMutations]] objects.
    */
  def mapMutations[A: ClassTag](ctx: PartitionGroupContext, mode: MutationsFilteringMode,
      mappingFunc: MutationTuple => A): MappedMutations[A]
}

/** Companion object providing a factory method to instantiate a specific PartitionGroupManager
  * implementation based on the Spark configuration object passed in.
  *
  * The TwoLevelPartitionGroupManager is the current default implementation that is chosen.
  */
object PartitionGroupManager {
  def apply(conf: SparkConf, dataRepo: String, job: String): PartitionGroupManager = {
    conf.get("mstore.partitionGroupManager", "TwoLevelPartitionGroupManager")
      match {
        case "TwoLevelPartitionGroupManager" => new TwoLevelPartitionGroupManager(conf, dataRepo,
          job)
        case unsupported => throw new IllegalArgumentException(
          "Unsupported partition group manager: " + unsupported)
      }
  }
}

/** Models how mutations should be filtered. */
sealed trait MutationsFilteringMode

/** Specifies that mutations be filtered inline with filters compaction.
  *
  * This mode should be used when running mutations compaction inline with filters compaction.
  *
  * @param filters A map of compacted filters returned from a previous call to compactFilters().
  */
case class InlineWithFiltersCompaction(filters: Map[String, CompactedFilter])
extends MutationsFilteringMode

/** Specifies that mutations be filtered offline i.e., separately from filters compaction.
  *
  * This mode should be used for offline compaction, recover as well as other mapping operations
  * such as masking and sampling.
  *
  * @param snapshot The snapshot from which to read the mutations and the filters.
  */
case class Offline(snapshot: String) extends MutationsFilteringMode
