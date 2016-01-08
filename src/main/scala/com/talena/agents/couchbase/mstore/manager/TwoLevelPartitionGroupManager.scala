package com.talena.agents.couchbase.mstore.manager

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore._
import com.talena.agents.couchbase.mstore.strategy._

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
    // Look up the strategies specified for the deduplication and compaction operations
    val deduplicationStrategy = FilterDeduplicationStrategy(
      conf.get("mstore.filterDeduplicationStrategy", "SparkSQL"))
    val compactionStrategy = FilterCompactionStrategy(
      conf.get("mstore.filterCompactionStrategy", "SparkRDD"))
    logger.info(s"Using filter deduplication strategy: $deduplicationStrategy and " +
      "filter compaction strategy: $compactionStrategy for $ctx")

    val l0Loc = l0Location(ctx)
    val l1Loc = l1Location(ctx)
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
    val l0DedupedFilter = l0Filter.deduplicate(deduplicationStrategy)

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
      .map(f => f.compact(l0DedupedFilter.broadcastKeys(), rblog.map(r => r.broadcast()),
        compactionStrategy))
      .getOrElse({
        logger.info(s"L1 filter not found (possibly because this is the first compaction run). " +
          "Creating a new L1 filter from the deduplicated L0 filter.")
        CompactedFilter(l0DedupedFilter.rdd, l0DedupedFilter.props)
      })

    logger.info("Finished compacting L1 filter for $ctx")
    Map(("l1CompactedFilter", l1CompactedFilter))
  }

  override def persistCompactedFilters(ctx: PartitionGroupContext,
      filters: Map[String, CompactedFilter], dest: String): Unit = {
    val l1Loc = l1LocationUsingPrefix(dest)(ctx)
    logger.info(s"Persisting compacted L1 filter for $ctx to $l1Loc")
    filters
      .getOrElse("l1CompactedFilter", throw new IllegalArgumentException("Invalid filter"))
      .persist(l1Loc)
  }

  override def compactMutations(ctx: PartitionGroupContext, mode: MutationsFilteringMode)
  : Option[Map[String, MappedMutations[MutationTuple]]] = {
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
    l1PersistedMutationsFilterPairFor(ctx)
      .filter({ case (m, f) => m.source.files.length > l1CompactionThreshold })
      .map({ case (m, f) => mapMutations[MutationTuple](m, f, mode, { m: MutationTuple => m }) })
      .map({ m =>
        logger.info("Finished compacting L1 mutations for $ctx")
        Map(("l1CompactedMutations", m))
      })
  }

  override def persistCompactedMutations(ctx: PartitionGroupContext,
      mutations: Map[String, MappedMutations[MutationTuple]], dest: String): Unit = {
    import org.apache.hadoop.io.NullWritable

    val l1Loc = l1LocationUsingPrefix(dest)(ctx)
    logger.info(s"Persisting compacted L1 mutations for $ctx to $l1Loc")

    mutations
      .getOrElse("l1CompactedMutations", throw new IllegalArgumentException("Invalid mutations"))
      .rdd
      .map(m => (NullWritable.get(), m))
      .saveAsSequenceFile(l1Loc)
  }

  override def mapMutations[A: ClassTag](ctx: PartitionGroupContext, mode: MutationsFilteringMode,
      mappingFunc: MutationTuple => A): MappedMutations[A] = {
    /** Open the partition group if the both mutation and filter files are found and invoke
      * mapMutations() with the provided mapping function.
      */
    val (m, f) = l1PersistedMutationsFilterPairFor(ctx)
      .getOrElse(throw new IllegalArgumentException("No mutation or filter files found: " + ctx))

    val mappedMutations = mapMutations(m, f, mode, mappingFunc)
    logger.info(s"Finished mapping mutations for $ctx")

    mappedMutations
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
    val mappingStrategy = MutationsMappingStrategy[A](
      conf.get("mstore.mutationsMappingStrategy", "SparkRDD"))
    logger.info(s"Using mutations mapping strategy: $mappingStrategy")

    mutations.map[A](filter, mappingFunc, mappingStrategy)
  }

  /** Convenience method to extract mutations and filter from a PartitionGroupContext object.
    *
    * @param ctx A PartitionGroupContext object
    * @return Some(mutations, filter) if BOTH mutations and filter are found
    *         None if NEITHER mutations NOR filter are found
    */
  private def l1PersistedMutationsFilterPairFor(ctx: PartitionGroupContext)
  : Option[(PersistedMutations, Filter)] = {
    PartitionGroup(PartitionGroupProps(ctx.id, ctx.bucket, l1Location(ctx), ctx.env)) match {
      case Some(PartitionGroup(m @ PersistedMutations(_, _, _), f, _)) => Some(m, f)
      case _ => None
    }
  }

  /** Shortcuts for the possible usages of the curried location() function */
  private lazy val l0Location  = locationWithoutPrefix(l0.toString)
  private lazy val l1Location = locationWithoutPrefix(l1.toString)
  private def l0LocationUsingPrefix(prefix: String) = locationUsingPrefix(prefix)(l0.toString)
  private def l1LocationUsingPrefix(prefix: String) = locationUsingPrefix(prefix)(l1.toString)

  /** Convenience methods for the partial application of the curried location() function */
  private lazy val locationWithoutPrefix = location("") _
  private def locationUsingPrefix(prefix: String) = location(prefix) _

  /** A curried function to compute the location for a partition group's files given a level and an
    * optional prefix.
    *
    * The prefix is used by the persist() class of APIs that include a "destination" parameter
    * specifying where the outputs must be saved.
    *
    * The output of this function will be of the form:
    * - /prefix/dataRepo/job/bucket/level/pgid if the prefix is specified OR
    * - //dataRepo/job/bucket/level/pgid if the prefix is not specified
    */
  private def location(prefix: String)(level: String)(ctx: PartitionGroupContext): String =
    Array(prefix, dataRepo, job, ctx.bucket, level, ctx.id.toString)
      .foldLeft("")((a, b) => a + "/" + b)

  /** Model the levels as case objects so we don't have to deal with strings */
  sealed trait level
  case object l0 extends level
  case object l1 extends level
}
