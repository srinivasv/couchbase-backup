package com.talena.agents.couchbase.cstore.manager

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.cstore._
import com.talena.agents.couchbase.mstore._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** A two level (L0/L1) partition group manager. */
class TwoLevelPartitionGroupManagerNew(conf: SparkConf, dataRepo: String, job: String)
extends PartitionGroupManagerNew(conf, dataRepo, job) {
  override def compactFilters(ctx: PartitionGroupContext): Environment[Unit] = {
    Environment(env => {
      // Compute the L0 and L1 paths for this partition group
      val ext = MStoreProps.FilterFileExtension(conf)
      val l0F = l0Path(dataRepo, job, ctx, ext)
      val l1F = l1Path(dataRepo, job, ctx, ext)
      logger.info(s"L0 path: $l0, L1 path: $l1")

      // Open the L0 filter for this partition group. Abort if the open fails.
      val l0Filter = CStore.openFilter(l0F)(env)
        .getOrElse(throw new IllegalStateException("L0 filter file missing: " + ctx))

      // Open the L1 filter for this partition group.
      val l1Filter = CStore.openFilter(l1F)(env).getOrElse(env.sc.emptyRDD[FilterTuple])

      // Open the rblog for this partition group.
      val rExt = MStoreProps.RBLogFileExtension(conf)
      val l0R = l0Path(dataRepo, job, ctx, rExt)
      val rblog = CStore.openRBLog(l0R)(env)

      val compactedL1Filter = filterCompactionPipeline(l0Filter, l1Filter)
      CStore.persist(compactedL1Filter.get(), "/tmp/somepath")
    })
  }

  private def filterCompactionPipeline(l0: RDD[FilterTuple], l1: RDD[FilterTuple])
  : State[RDD[FilterTuple]] = {
    import CStore.{broadcast, deduplicate}
    import CStore.FilterKeysBroadcaster
    for {
      l0a <- State(l0)
      l0b <- deduplicate(l0a)
      l0c <- broadcast(l0b)
      l1a <- State(l1) if l1a.filter(t => !l0c.value(t.key()))
    } yield(l1a)
  }
  //override def persistCompactedFilters(ctx: PartitionGroupContext,
  def persistCompactedFilters(ctx: PartitionGroupContext,
      filters: Map[String, CompactedFilter], tmpLoc: String): Unit = {
    val ext = MStoreProps.FilterFileExtension(conf)
    val l1 = l1PathWithPrefix(tmpLoc)(dataRepo, job, ctx, ext)
    logger.info(s"Persisting compacted L1 filter for $ctx to path $l1")

    filters
      .getOrElse("l1CompactedFilter", throw new IllegalArgumentException("Invalid filter"))
      .persist(l1)
  }

  // override def compactMutations(ctx: PartitionGroupContext, mode: MutationsFilteringMode)
  // : Unit = {
  //   /** Open the partition group and do the following if the both mutation and filter files are
  //     * found:
  //     * - Check whether the number of mutation files exceeds the compaction threshold.
  //     * - If so, invoke mapMutations() with an identity function that maps a mutation tuple to
  //     *   itself. This allows us to use the mapMutations() method for both compaction as well as for
  //     *   actual mapping operations.
  //     * - Return a Map object with a sole entry of the resulting MappedMutations object
  //     *
  //     * If compaction does not occur, return None
  //     */
  //   val l1CompactionThreshold = MStoreProps.TwoLevelPartitionGroupManagerL1CompactionThreshold(conf)
  //     .toInt
  //   println(s"Using mutations compaction threshold of $l1CompactionThreshold files for $ctx")
  //   logger.info(s"Using mutations compaction threshold of $l1CompactionThreshold files for $ctx")

  //   val mappedMutations = mapMutationsOnCondition[MutationTuple](ctx, mode)(m => m)((m, _) => {
  //     val numFiles = m.source.files.length
  //     println(s"$numFiles mutation files found for $ctx in L1")
  //     logger.info(s"$numFiles mutation files found for $ctx in L1")
  //     numFiles > l1CompactionThreshold
  //   })

  //   mappedMutations.map(m => {
  //     println(s"Finished compacting L1 mutations for $ctx")
  //     logger.info(s"Finished compacting L1 mutations for $ctx")
  //     Map(("l1CompactedMutations", CompactedMutations(m.rdd, m.env)))
  //   })
  // }

  // override def persistCompactedMutations(ctx: PartitionGroupContext,
  //     mutations: Map[String, CompactedMutations], tmpLoc: String): Unit = {
  //   val ext = MStoreProps.MutationsFileExtension(conf)
  //   val l1 = l1PathWithPrefix(tmpLoc)(dataRepo, job, ctx, ext)
  //   logger.info(s"Persisting compacted L1 mutations for $ctx")

  //   mutations
  //     .getOrElse("l1CompactedMutations", throw new IllegalArgumentException("Invalid mutations"))
  //     .persist(l1)
  // }

  def l0Path = path(l0)(None, None) _
  def l1Path = path(l1)(None, None) _
  def l1PathWithPrefix(prefix: String) = path(l1)(Some(prefix), None) _
  def l1PathForSnapshot(snapshot: String) = path(l1)(None, Some(snapshot)) _

  private def path(level: level)(prefix: Option[String], snapshot: Option[String])
      (dataRepo: String, job: String, ctx: PartitionGroupContext, ext: String): String = {
    Utils.buildFSPath(List(
      prefix, Some(dataRepo), Some(job), snapshot, Some(ctx.bucket), Some(level.toString)
    )) + ctx.id + "*" + ext
  }
}

/** Model the levels as case objects so we don't have to deal with strings */
sealed trait level
case object l0 extends level
case object l1 extends level
