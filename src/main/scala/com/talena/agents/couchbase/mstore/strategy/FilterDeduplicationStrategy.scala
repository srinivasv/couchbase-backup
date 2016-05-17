package com.talena.agents.couchbase.mstore.strategy

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

object FilterDeduplicationStrategy extends LazyLogging {
  def apply(env: Env): Filter => DeduplicatedFilter = {
    MStoreProps.FilterDeduplicationStrategy(env.conf) match {
      case "SparkSQL" =>
        logger.info(s"Using filter deduplication strategy: SparkRDD")
        usingSparkSQL(env) _
      case unsupported => throw new IllegalArgumentException(
        "Unsupported filter deduplication strategy: " + unsupported)
    }
  }

  /** A wrapper to convert FilterTuple to a Scala case class to enable implicit Spark DataFrame
    * transformations on it.
    */
  case class ShadowFilterTuple(pid: Short, key: String, seqno: Long)

  /** Uses Spark SQL operations for deduplicating a filter.
    *
    * @param filter The filter that will be deduplicated
    * @param env A reference to an active Env object for involing Spark operations
    * @return The deduplicated filter as a new DeduplicatedFilter object
    */
  private def usingSparkSQL(env: Env)(filter: Filter): DeduplicatedFilter = {
    filter match {
      case PersistedFilter(rdd, _, _) =>
        import env.sqlCtx.implicits._
        logger.info(s"Deduplicating filter using SparkSQL")
        DeduplicatedFilter(
          rdd
            .map(v => ShadowFilterTuple(v.partitionId(), v.key(), v.seqNo()))
            .toDF("pid", "key", "seqno")
            .groupBy($"pid", $"key")
            .max("seqno")
            .toDF("pid", "key", "seqno")
            .map(r => new FilterTuple(r.getShort(0), r.getLong(2), r.getString(1))),
          env)
      case unsupported => throw new IllegalArgumentException(
        "Unsupported type: " + unsupported)
    }
  }
}
