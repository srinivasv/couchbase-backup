package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.mstore.strategy.FilterCompactionStrategy
import com.talena.agents.couchbase.mstore.strategy.FilterDeduplicationStrategy
import com.talena.agents.couchbase.mstore.strategy.MutationsMappingStrategy

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map

/** A PartitionGroup is the smallest unit of computation in MStore and comprises a set of Couchbase
  * bucket partitions that are always backed up, compacted, mapped and restored as a single unit.
  *
  * PartitionGroup is modeled using what are called Algebraic Data Types (ADTs) in functional
  * programming. The idea is to define a rigorous type system that mimics the business functions
  * being modeled (compaction, recovery, masking/sampling operations in our case) and then let the
  * compiler do static type checks on our code. This, along with pattern matching, will ensure that
  * buggy code doesn't even get compiled, thus eliminating large classes of bugs altogether.
  *
  * Below is a high level description of the data model using the following terminology:
  * "=>" implies an "is" or "is made up of" relationship between the LHS and the RHS.
  * "||" implies an "is one of" relationship between the LHS and RHS. In other words, the RHS is a
  *      set of possible alternatives for the LHS.
  *
  * PartitionGroup => (Mutations, Filter, Option(RBLog))
  *
  *
  * Mutations => PersistedMutations               Mutations that have been read off persistent
  *                                               storage i.e. they were preivously persisted there.
  *           || MappedMutations                  Mutations that have been read and transformed by
  *                                               applying a map function one or more times.
  *
  * Filter => BroadcastableFilter                 A filter that can be broadcasted for compacting
  *                                               filters and mutations.
  *        || BroadcastedFilter                   A filter than has been broadcasted for such a
  *                                               purpose.
  *
  * BroadcastableFilter => PersistedFilter        A filter that has been read off persistent storage
  *                                               similar to PersistedMutations.
  *                     || DeduplicatedFilter     A PersistedFilter that has been deduplicated to
  *                                               eliminate duplicate keys we may have received
  *                                               during data movement. An L0 filter is an example
  *                                               of a filter that could be deduplicated.
  *                     || CompactedFilter        A PersistedFilter (persisted in a previous backup
  *                                               run) that has been compacted in the current run.
  *                                               An L1 filter is an example of a filter that could
  *                                               be compacted.
  *
  * BroadcastedFilter => BroadcastedKeys          A filter whose keys have been broadcast to allow
  *                                               compacting a higher level filter such as L1 or L2.
  *                   || BroadcastedSeqnoTuples   A filter whose seqno tuples (partition id, uuid
  *                                               and seqno) have been broadcast to allow filtering
  *                                               of mutations that are no longer live.
  *
  * RBLog => BroadcastableRBLog                   An rblog that can be broadcasted for compacting a
  *                                               higher level filter.
  *       || BroadcastedRBLog                     An rblog that has been broadcast for such a
  *                                               purpose.
  *
  * BroadcastableRBLog => PersistedRBLog          An rblog that has been read off persistent storage
  *                                               similar to PersistedMutations and PersistedFilter.
  *
  *
  * Additionally, the Mutations, Filter, BroadcastableFilter and BroadcastableRBLog classes also
  * encapsulate methods that are required by their respective subclasses. The important ones and
  * their descriptions are as follows:
  *
  * Mutations:
  * - map(filter, mappingFunc, strategy)          Filters mutations using the given filter and
  *                                               transforms them by applying the given function and
  *                                               the strategy. Returns a transformed
  *                                               MappedMutations object.
  *
  * Filter:
  * - deduplicate(strategy)                       Deduplicates this filter using the given strategy.
  *                                               Returns a new Deduplicated filter.
  * - compact(filter, rblog, strategy)            Compacts this filter using the given filter and
  *                                               strategy. Returns a new CompactedFilter.
  *
  * BroadcastableFilter
  * - broadcastKeys()                             Broadcasts just the keys of this filter to all
  *                                               nodes. Returns a new Broadcasted object containing
  *                                               a reference to the broadcasted keys. Used as a
  *                                               pre-step for compaction of higher level (L1/L2)
  *                                               filters.
  * - broadcastSeqnoTuples()                      Broadcasts the (partitionid, partitionuuid, seqno)
  *                                               tuples to all nodes. Returns a new Broadcasted
  *                                               object containing a reference to the broadcasted
  *                                               tuples. Used as a pre-step for filtering of
  *                                               mutations during compaction, recovery and any
  *                                               other mapping operations.
  *
  * BroadcastableRBLog
  * - broadcast()                                 Broadcasts the (partitionid, partitionuuid, seqno)
  *                                               tuples of the rblog to all nodes. Returns a new
  *                                               BroadcastedRBLog object containing a reference to
  *                                               the broadcasted tuples. Used as a pre-step for
  *                                               compaction of higher level filters.
  *
  */
case class PartitionGroup(mutations: Mutations, filter: Filter, rblog: Option[RBLog])
extends LazyLogging

/** Companion object for the PartitionGroup class */
object PartitionGroup extends LazyLogging {
  def apply(props: PartitionGroupProps): Option[PartitionGroup] = {
    val mutations = Mutations(props)
    val filter = Filter(props)
    val rblog = RBLog(props)

    /** Only the following two are valid scenarios. Throw an exception for all others.
      * - Valid mutations AND filter files are found, the rblog being optional.
      *   In this case we return Some(PartitionGroup(...)).
      * - No mutations OR filter OR rblog files are found.
      *   IN this case we return None.
      *   This scenario will be applicable to the L1/L2 levels if there has only been ONE backup run
      *   so far and this is the first time that any compaction is running.
      */
    (mutations, filter, rblog) match {
      case (Some(m), Some(f), r) => Some(PartitionGroup(m, f, r))
      case (None, None, None) => None
      case unexpected => throw new IllegalStateException(
        "Unexpected state for partition group: " + props + " " + unexpected)
    }
  }
}

sealed abstract class Mutations(props: PartitionGroupProps) extends LazyLogging {
  /** Maps each MutationTuple in this mutations object to type A by applying a specified function
    * and using a specified strategy.
    *
    * Does not do the actual transformation itself but rather invokes the strategy object passed in
    * to get that done.
    *
    * @tparam A The type of the transformed tuples.
    * @param filter The filter to use for filtering out stale mutations.
    * @param mappingFunc The function to use to do the mapping or transformation.
    * @param strategy The strategy to use for the mapping.
    * @return A MappedMutations object of type A containing the transformed mutations.
    */
  def map[A](filter: Filter, mappingFunc: MutationTuple => A,
      strategy: MutationsMappingStrategy[A]): MappedMutations[A] = {
    strategy(this, filter, mappingFunc, props.env)
  }
}

case class PersistedMutations(rdd: RDD[MutationTuple], source: Files, props: PartitionGroupProps)
extends Mutations(props)
case class MappedMutations[A](rdd: RDD[A], props: PartitionGroupProps) extends Mutations(props)

object Mutations extends LazyLogging {
  def apply(props: PartitionGroupProps): Option[PersistedMutations] = {
    def open(glob: Path, env: Env, files: Array[FileStatus]): PersistedMutations = {
      val rdd = env.sparkCtx.sequenceFile[Text, MutationTuple](glob.toString())
        .map({ case (_, v) => v })
      PersistedMutations(rdd, Files(files, SequenceFile), props)
    }

    val glob = new Path(props.location + "/*.mutations")
    Utils.listFiles(props.env.fs, glob).map(f => open(glob, props.env, f))
  }
}

sealed abstract class Filter(props: PartitionGroupProps) extends LazyLogging {
  /** Deduplicates this filter by eliminating stale duplicate keys.
    *
    * Invokes the strategy object passed in to get the actual deduplication done.
    *
    * @param strategy The strategy to use for the mapping.
    * @return A DeduplicatedFilter object.
    */
  def deduplicate(strategy: FilterDeduplicationStrategy): DeduplicatedFilter = {
    strategy(this, props.env)
  }

  /** Compacts this filter using a specified filter.
    *
    * Invokes the strategy object passed in to get the actual compaction done.
    *
    * @param filter The filter to use for the compaction.
    * @param rblog An optional rblog to use eliminate stale seqnos due to a rollback event.
    * @param strategy The strategy to use for the mapping.
    * @return A CompactedFilter object.
    */
  def compact(filter: Filter, rblog: Option[RBLog], strategy: FilterCompactionStrategy)
  : CompactedFilter = {
    strategy(this, filter, rblog, props.env)
  }
}

object Filter extends LazyLogging {
  def apply(props: PartitionGroupProps): Option[PersistedFilter] = {
    def open(glob: Path, env: Env, files: Array[FileStatus]): PersistedFilter = {
      val rdd = env.sparkCtx.sequenceFile[Text, FilterTuple](glob.toString())
        .map({ case (_, v) => v })
      PersistedFilter(rdd, Files(files, SequenceFile), props)
    }

    val glob = new Path(props.location + "/*.filter")
    Utils.listFiles(props.env.fs, glob).map(f => open(glob, props.env, f))
  }
}

sealed abstract class BroadcastableFilter(props: PartitionGroupProps) extends Filter(props) {
  /** Broadcasts the keys of this filter to all nodes (a pre-step for a higher level filter
    * compaction).
    *
    * Only broadcasting a DeduplicatedFilter is allowed, which means that before invoking this
    * method the caller should have deduplicated the filter they want broadcasted.
    *
    * The actual broadcast is done using Spark's broadcast variables feature.
    *
    * This is an example of how ADTs in combination with pattern matching can be used to embed
    * business logic into the type system itself, which can then be enforced by the compiler to
    * detect unexpected conditions and code paths early on.
    *
    * @return A BroadcastedKeys object.
    */
  def broadcastKeys(): BroadcastedKeys = {
    this match {
      case DeduplicatedFilter(rdd, props) => BroadcastedKeys(props.env.sparkCtx.broadcast(
        rdd
          .map(t => t.key())
          .collect()
          .toSet),
        props)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported filter type for keys broadcast: " + unsupported)
    }
  }

  /** Broadcasts the (partition id, partition uuid, seqno) tuples of this filter to all nodes (a
    * pre-step for mutations filtering during compaction, recovery and mapping operations).
    *
    * Only broadcasting a PersistedFilter is allowed.
    * TODO: Add support for broadcasting a CompactedFilter (the use case being that both filters and
    * mutations are compacted together inline)
    *
    * The actual broadcast is done using Spark's broadcast variables feature.
    *
    * @return A BroadcastedSeqnoTuples object.
    */
  def broadcastSeqnoTuples(): BroadcastedSeqnoTuples = {
    this match {
      case PersistedFilter(rdd, _, props) => BroadcastedSeqnoTuples(props.env.sparkCtx.broadcast(
        rdd
          .map(t => (t.partitionId(), t.uuid(), t.seqNo()))
          .collect()
          .toSet),
        props)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported filter type for seqno tuples broadcast: " + unsupported)
    }
  }

  /** Saves this filter at the specified location.
    *
    * Only a CompactedFilter is allowed as there is no use case for saving other kinds of filters at
    * this time.
    */
  def persist(location: String): Unit = {
    this match {
      case CompactedFilter(rdd, _) => rdd
        .map(t => (NullWritable.get(), t))
        .saveAsSequenceFile(location)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported filter type for persist: " + unsupported)
    }
  }
}

case class PersistedFilter(rdd: RDD[FilterTuple], source: Files, props: PartitionGroupProps)
extends BroadcastableFilter(props)
case class DeduplicatedFilter(rdd: RDD[FilterTuple], props: PartitionGroupProps)
extends BroadcastableFilter(props)
case class CompactedFilter(rdd: RDD[FilterTuple], props: PartitionGroupProps)
extends BroadcastableFilter(props)

sealed abstract class BroadcastedFilter(props: PartitionGroupProps) extends Filter(props)

case class BroadcastedKeys(bcast: Broadcast[Set[String]], props: PartitionGroupProps)
extends BroadcastedFilter(props)
case class BroadcastedSeqnoTuples(bcast: Broadcast[Set[(Short, Long, Long)]],
  props: PartitionGroupProps) extends BroadcastedFilter(props)

sealed abstract class RBLog(props: PartitionGroupProps) extends LazyLogging
object RBLog extends LazyLogging {
  def apply(props: PartitionGroupProps): Option[PersistedRBLog] = {
    def open(glob: Path, env: Env, files: Array[FileStatus]): PersistedRBLog = {
      val rdd = env.sparkCtx.sequenceFile[Text, RBLogTuple](glob.toString())
        .map({ case (_, v) => v })
      PersistedRBLog(rdd, Files(files, SequenceFile), props)
    }

    val glob = new Path(props.location + "/*.rblog")
    Utils.listFiles(props.env.fs, glob).map(f => open(glob, props.env, f))
  }
}

sealed abstract class BroadcastableRBLog(props: PartitionGroupProps) extends RBLog(props) {
  /** Broadcasts the tuples of this rblog to all nodes (a pre-step for higher level filter
    * compaction).
    *
    * Only a PersistedRBLog may be broadcasted.
    *
    * The actual broadcast is done using Spark's broadcast variables feature.
    *
    * @return A BroadcastedRBLog object.
    */
  def broadcast(): BroadcastedRBLog = {
    this match {
      case PersistedRBLog(rdd, _, props) => BroadcastedRBLog(props.env.sparkCtx.broadcast(
        rdd
          .map(t => ((t.partition(), t.uuid()), t.seqno()))
          .collectAsMap()),
        props)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported rollback log type for broadcast: " + unsupported)
    }
  }
}

case class PersistedRBLog(rdd: RDD[RBLogTuple], source: Files, props: PartitionGroupProps)
extends BroadcastableRBLog(props)

case class BroadcastedRBLog(bcast: Broadcast[Map[(Short, Long), Long]], props: PartitionGroupProps)
extends RBLog(props)

/** Encapsulates the set of files (along with their file format) backing the RDD in case of
  * PersistedMutations, PersistedFilter and PersistedRBLog classes.
  */
case class Files(files: Array[FileStatus], format: FileFormat)

// Supported file formats
sealed trait FileFormat
case object CSV extends FileFormat
case object JSON extends FileFormat
case object SequenceFile extends FileFormat

/** A set of props that are stored along with each object in the PartitionGroup ADT hierarchy.
  *
  * @param id An ID for this parition group.
  * @param bucket The Couchbase bucket to which this partition group belongs.
  * @param location The HDFS location where the files for this partition group can be found.
  * @param env An instance of [[com.talena.agents.couchbase.mstore.Env]] that was used to
  *            instantiate this partition group.
  */
case class PartitionGroupProps(id: Int, bucket: String, location: String, env: Env)
