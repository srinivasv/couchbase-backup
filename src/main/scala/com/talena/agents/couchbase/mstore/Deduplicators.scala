package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

object Deduplicators extends LazyLogging {
  implicit object FilterDeduplicator extends Deduplicable[FilterTuple] {
    override def deduplicate(rdd: RDD[FilterTuple]): MState[RDD[FilterTuple]] = {
      def usingSparkRDD(rdd: RDD[FilterTuple]): RDD[FilterTuple] = {
        rdd
          .map(f => (f.key(), f))
          .reduceByKey((f1, f2) => { if(f1.seqNo > f2.seqNo) f1 else f2 })
          .map({ case (k, f) => f})
      }

      val conf = rdd.sparkContext.getConf
      val rdd1 = MStoreProps.FilterDeduplicationStrategy(conf) match {
        case "SparkRDD" =>
          logger.info(s"Using filter deduplication strategy: SparkRDD")
          usingSparkRDD(rdd)
        case unsupported => throw new IllegalArgumentException(
          "Unsupported filter deduplication strategy: " + unsupported)
      }

      MState(rdd1)
    }
  }
}
