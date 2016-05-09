package com.talena.agents.couchbase.mstore.strategy

import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

object FilterCompactionStrategy extends LazyLogging {
  def apply(env: Env): (Filter, Filter, Option[RBLog]) => CompactedFilter = {
    MStoreProps.FilterCompactionStrategy(env.conf) match {
      case "SparkRDD" =>
        logger.info(s"Using filter compaction strategy: SparkRDD")
        usingSparkRDD(env) _
      case unsupported => throw new IllegalArgumentException(
        "Unsupported filter compaction strategy: " + unsupported)
    }
  }

  /** Uses Spark RDD operations for compacting a filter.
    *
    * For correctness reasons, compaction is done only if all of the following conditions are met,
    * which are specified by means of pattern matching so that the compiler can enforce them at
    * compile time itself:
    * - Filter being compacted is a PersistedFilter. There is no use case for other filters to be
    *   compacted.
    * - Keys of the reference filter must have already been broadcasted. This check is necessary as
    *   otherwise we will end up with incorrect results.
    * - If present, the rblog must also have been broadcasted for the same reason as above.
    *
    * We use Spark's mapPartitions() API which allows us to process an entire partition at a time.
    *
    * Compaction steps:
    * - Read the broadcasted reference filter keys and rblog tuples into scala variables newKeys and
    *   staleSeqnos, respectively. newKeys is a Set data structure while staleSeqnos is a HashMap,
    *   where the key is a (partition id, uuid) tuple and the value is the max valid seqno for it.
    * - For each tuple f in the old filter, check whether:
    * -- The key is still valid i.e., NOT present in newKeys.
    * -- The seqno is still valid i.e., NOT present in staleSeqnos OR is less than the seqno in it.
    * - If both these conditions are met, "yield" f.
    *
    * TODO: The code can be refactored to remove some duplication.
    *
    * @param oldF The filter that will be compacted
    * @param newF The filter that will be used as a reference
    * @param rblog An optional rollback log to consider during compaction
    * @param env A reference to an active Env object for involing Spark operations
    * @return The compacted oldF filter as a new CompactedFilter object
    */
  private def usingSparkRDD(env: Env)(oldF: Filter, newF: Filter, rblog: Option[RBLog])
  : CompactedFilter = {
    (oldF, newF, rblog) match {
      case (PersistedFilter(oldRDD, _, _), BroadcastedKeys(bcastNewKeys, _),
        Some(BroadcastedRBLog(bcastRBlog, _))) => CompactedFilter(
          oldRDD.mapPartitions({ iter =>
            logger.info(s"Compacting filter using provided filter and rollback log")

            val newKeys = bcastNewKeys.value
            val staleSeqnos = bcastRBlog.value
            def isValidKey(k: String) = !newKeys(k)
            def isValidSeqno(p: Short, u: Long, s: Long) = s < staleSeqnos.getOrElse((p, u), s + 1)

            for {
              f <- iter if isValidKey(f.key()) && isValidSeqno(f.partitionId(), f.uuid(), f.seqNo())
            } yield f
          }, preservesPartitioning = true),
          env)

      case (PersistedFilter(oldRDD, _, _), BroadcastedKeys(bcastNewKeys, _), None) =>
        CompactedFilter(
          oldRDD.mapPartitions({ iter =>
            logger.info(s"Compacting filter using provided filter. No rollback log specified.")

            val newKeys = bcastNewKeys.value
            def isValidKey(k: String) = !newKeys(k)

            for {
              f <- iter if isValidKey(f.key())
            } yield f
          }, preservesPartitioning = true),
          env)

      case unsupported => throw new IllegalArgumentException("Unsupported types: " + unsupported)
    }
  }
}
