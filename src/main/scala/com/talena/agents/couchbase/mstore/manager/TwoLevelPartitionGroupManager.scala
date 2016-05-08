package com.talena.agents.couchbase.mstore.manager

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore._
import com.talena.agents.couchbase.mstore.strategy._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}

import org.apache.spark.SparkConf

import scala.reflect.ClassTag

/** A two level (L0/L1) partition group manager. */
class TwoLevelPartitionGroupManager(conf: SparkConf, dataRepo: String, job: String)
extends PartitionGroupManager(conf, dataRepo, job) {
  override def addPartitionGroup(ctx: PartitionGroupContext): Unit = {
    throw new UnsupportedOperationException("addPartitionGroup() not supported")
  }

  override def removePartitionGroup(ctx: PartitionGroupContext): Unit = {
    throw new UnsupportedOperationException("removePartitionGroup() not supported")
  }

  /**
    * Only the L1 filter needs to be compacted. Steps are as follows:
    * - Open the L0 filter for the partition group context passed to us. L0 filter must be present.
    *   Abort otherwise.
    * - Open the L1 filter. If it isn't present, it means this is the first time compaction is being
    *   run.
    * - Open the rollback log if it is present.
    * - Deduplicate the L0 filter using the specified strategy specified in the Spark conf object.
    * - Compact the L1 filter using the deduplicated L0 filter, the rollback log and the strategy
    *   specified in the Spark conf object.
    * - Return a Map object with a sole entry containing the compacted L1 filter.
    */
  override def compactFilters(ctx: PartitionGroupContext): Map[String, CompactedFilter] = {
    // Compute the L0 and L1 locations for this partition group
    val l0Loc = Utils.buildFSLocation(dataRepo :: job :: ctx.bucket :: l0.toString :: Nil)
    val l1Loc = Utils.buildFSLocation(dataRepo :: job :: ctx.bucket :: l1.toString :: Nil)
    logger.info(s"L0 location: $l0Loc, L1 location: $l1Loc")

    // Open the L0 filter for this partition group. Abort if the open fails.
    val l0Filter = Filter(PartitionGroupProps(ctx.id, ctx.bucket, l0Loc, ctx.env))
      .getOrElse(throw new IllegalStateException("L0 filter file missing: " + ctx))

    // Open the L1 filter for this partition group.
    val l1Filter = Filter(PartitionGroupProps(ctx.id, ctx.bucket, l1Loc, ctx.env))

    // Open the rblog for this partition group.
    val rblog = RBLog(PartitionGroupProps(ctx.id, ctx.bucket, l0Loc, ctx.env))

    // Deduplicate the L0 filter
    logger.info(s"Deduplicating L0 filter")
    val l0DedupedFilter = l0Filter.deduplicate()

    /*
     * Compact the L1 filter using the deduplicated L0 filter and the rblog. This will result in a
     * new CompactedFilter object.
     *
     * If the L1 filter doesn not exist, which means there is nothing to compact, which in turn
     * means that compaction is being run on this partition group for the first time, we must save
     * deduplicated L0 filter itself as the L1 filter. Hence, we simply return a CompactedFilter
     * object constructed by reusing the deduplicated L1 filter's RDD.
     */
    logger.info(s"Compacting L1 filter")
    val l1CompactedFilter = l1Filter
      .map(f => f.compact(l0DedupedFilter.broadcastKeys(), rblog.map(r => r.broadcast())))
      .getOrElse({
        logger.info(s"L1 filter not found (possibly because this is the first compaction run). " +
          "Creating a new L1 filter from the deduplicated L0 filter.")
        CompactedFilter(l0DedupedFilter.rdd, l0DedupedFilter.props)
      })

    logger.info("Finished compacting L1 filter for $ctx")
    Map(("l1CompactedFilter", l1CompactedFilter))
  }

  override def persistCompactedFilters(ctx: PartitionGroupContext,
      filters: Map[String, CompactedFilter], tmpLoc: String): Unit = {
    val loc = Utils.buildFSLocation(tmpLoc :: dataRepo :: job :: ctx.bucket :: l0.toString :: Nil)
    logger.info(s"Persisting compacted L1 filter for $ctx")

    filters
      .getOrElse("l1CompactedFilter", throw new IllegalArgumentException("Invalid filter"))
      .persist(loc)
  }

  override def moveCompactedFilters(ctx: PartitionGroupContext, tmpLoc: String): Unit = {
    val fs = ctx.env.fs
    val ext = ".filter"

    val file = ctx.id + ext
    val from = Utils.buildFSLocation(tmpLoc :: dataRepo :: job :: ctx.bucket :: l1.toString :: Nil) + file
    val to = Utils.buildFSLocation(dataRepo :: job :: ctx.bucket :: l1.toString :: Nil) + file

    logger.info(s"Moving compacted filter file for $ctx from $from to $to")
    fs.rename(new Path(from), new Path(to))
  }

  override def compactMutations(ctx: PartitionGroupContext, mode: MutationsFilteringMode)
  : Option[Map[String, CompactedMutations]] = {
    /** Look up the compaction threshold from the Spark conf object */
    val l1CompactionThreshold = conf.get(
      "mstore.twoLevelPartitionGroupManager.l1CompactionThreshold", "10").toInt
    logger.info(s"Using mutations compaction threshold of $l1CompactionThreshold files for $ctx")

    /** Open the partition group and do the following if the both mutation and filter files are
      * found:
      * - Check whether the number of mutation files exceeds the compaction threshold.
      * - If so, invoke mapMutations() with an identity function that maps a mutation tuple to
      *   itself. This allows us to use the mapMutations() method for both compaction as well as for
      *   actual mapping operations.
      * - Return a Map object with a sole entry of the resulting MappedMutations object
      *
      * If compaction does not occur, return None
      */
    val l1Loc = Utils.buildFSLocation(dataRepo :: job
      :: addSnapshot(mode, ctx.bucket :: l1.toString :: Nil))
    PartitionGroup(PartitionGroupProps(ctx.id, ctx.bucket, l1Loc, ctx.env))
      .filter({
        case PartitionGroup(m: PersistedMutations, _, _) =>
          m.source.files.length > l1CompactionThreshold
      })
      .map({
        case PartitionGroup(m: PersistedMutations, f, _) =>
          val m1 = mapMutations[MutationTuple](m, f, mode, { m: MutationTuple => m })
          logger.info("Finished compacting L1 mutations for $ctx")
          Map(("l1CompactedMutations", CompactedMutations(m1.rdd, m1.props)))
      })
  }

  override def persistCompactedMutations(ctx: PartitionGroupContext,
      mutations: Map[String, CompactedMutations], tmpLoc: String): Unit = {
    val loc = Utils.buildFSLocation(tmpLoc :: dataRepo :: job :: ctx.bucket :: l1.toString :: Nil)
    logger.info(s"Persisting compacted L1 mutations for $ctx")

    mutations
      .getOrElse("l1CompactedMutations", throw new IllegalArgumentException("Invalid mutations"))
      .persist(loc)
  }

  override def moveCompactedMutations(ctx: PartitionGroupContext, tmpLoc: String): Unit = {
    val fs = ctx.env.fs
    val ext = ".mutations"

    val file = ctx.id + ext
    val from = Utils.buildFSLocation(tmpLoc :: dataRepo :: job :: ctx.bucket :: l1.toString :: Nil) + file
    val to = Utils.buildFSLocation(dataRepo :: job :: ctx.bucket :: l1.toString :: Nil) + file

    logger.info(s"Moving compacted mutations file for $ctx from $from to $to")
    fs.rename(new Path(from), new Path(to))
  }

  override def moveUncompactedMutations(ctx: PartitionGroupContext): Unit = {
    val fs = ctx.env.fs
    val ext = ".mutations"

    val file = ctx.id + ext
    val from = Utils.buildFSLocation(List(dataRepo, job, ctx.bucket, l1.toString)) + file
    val to = Utils.buildFSLocation(List(dataRepo, job, ctx.bucket, l1.toString)) + file

    logger.info(s"Moving uncompacted mutations file for $ctx from $from to $to")
    fs.rename(new Path(from), new Path(to))
  }

  override def mapMutations[A: ClassTag](ctx: PartitionGroupContext, mode: MutationsFilteringMode,
      mappingFunc: MutationTuple => A): MappedMutations[A] = {
    /** Open the partition group if the both mutation and filter files are found and invoke
      * mapMutations() with the provided mapping function.
      */
    val l1Loc = Utils.buildFSLocation(dataRepo :: job :: addSnapshot(mode, ctx.bucket :: l1.toString
      :: Nil))
    val (m, f) = PartitionGroup(PartitionGroupProps(ctx.id, ctx.bucket, l1Loc, ctx.env))
      .map({
        case PartitionGroup(m: PersistedMutations, f, _) => (m, f)
      })
      .getOrElse(throw new IllegalArgumentException("No mutation or filter files found: " + ctx))

    val m1 = mapMutations(m, f, mode, mappingFunc)
    logger.info(s"Finished mapping mutations for $ctx")

    m1
  }

  /** Filters mutations using a filter, then transforms them using a mapping functions.
    *
    * @param mutations The mutations to filter and transform.
    * @param filter The filter to use.
    * @param mode Whether the filtering should be done in the inline or offline mode.
    * @param mappingFunc The mapping function to use for the transformation.
    * @return A MappedMutations object representing the transformed mutations.
    */
  private def mapMutations[A: ClassTag](mutations: PersistedMutations, filter: Filter,
      mode: MutationsFilteringMode, mappingFunc: MutationTuple => A): MappedMutations[A] = {
    mutations.map[A](filter, mappingFunc)
  }

  private def addSnapshot(mode: MutationsFilteringMode, l: List[String]): List[String] = {
    mode match {
      case Offline(s) => s :: l
      case _ => l
    }
  }

  /** Model the levels as case objects so we don't have to deal with strings */
  sealed trait level
  case object l0 extends level
  case object l1 extends level
}
