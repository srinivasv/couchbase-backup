package com.talena.agents.couchbase.compactor

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

class Compactor(sqlCtx: SQLContext, l0: L0, l1: L1, l2: L2) {
  def removeDuplicatesInFilter(mutationStore: MutationStore) = {
    import sqlCtx.implicits._
    mutationStore match {
      case L0(DataFramePair(_, filter), props) => filter
        .groupBy($"pid", $"key")
        .max("seqno")
        .toDF("pid", "key", "seqno")
    }
  }

  def compact() = {
    val newFilter = removeDuplicatesInFilter(l0)
    newFilter.explain()
    newFilter.show()
  }
}
