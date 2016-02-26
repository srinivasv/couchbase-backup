package com.talena.agents.couchbase.compactor

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

class Compactor(sqlCtx: SQLContext, l0: L0, l1: L1, l2: L2) {
  def removeDuplicatesInFilter(mstore: MStore) = {
    import sqlCtx.implicits._
    mstore match {
      case L0(_, _, MData(_, Filter(filter, _, _), _)) => filter
        .groupBy($"pid", $"key")
        .max("seqno")
        .toDF("pid", "key", "seqno")
      case _ => throw new IllegalStateException("Not supported")
    }
  }

  def compact() = {
    val newFilter = removeDuplicatesInFilter(l0)
    newFilter.explain()
    newFilter.show()
  }
}
