package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore._
import com.talena.agents.couchbase.mstore.manager._

import org.apache.hadoop.fs.{FileSystem}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Driver {
  val dataRepo = "tmp/mstore/prod"
  val job = "sales"
  val bucket = "default"
  val ctxId = "1"

  def env(sc: SparkContext, sq: SQLContext) =
    Env(sc.getConf, sc, sq, FileSystem.get(sc.hadoopConfiguration))

  def mgr(sc: SparkContext) = PartitionGroupManager(sc.getConf, dataRepo, job)

  def ctx(env: Env) = PartitionGroupContext(ctxId, bucket, env)

  def loadFull(env: Env) = {
    val f1 = new FilterTuple(0, 1, "k1")
    val f2 = new FilterTuple(0, 2, "k2")
    val f3 = new FilterTuple(0, 3, "k1")

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)
    val m3 = new MutationTuple(f3)

    val mutations = env.sparkCtx.makeRDD(List(m1, m2, m3)).map(k => (NullWritable.get(), k))
    val filter = env.sparkCtx.makeRDD(List(f1, f2, f3)).map(k => (NullWritable.get(), k))

    val mgr = PartitionGroupManager(env.sparkCtx.getConf, dataRepo, job)
    val ctx = PartitionGroupContext(ctxId, bucket, env)

    val mPath = mgr.asInstanceOf[TwoLevelPartitionGroupManager].l0Path(
      dataRepo, job, ctx, ".mutations")
    println(s"Saving $mPath")
    mutations.saveAsSequenceFile(mPath)

    val fPath = mgr.asInstanceOf[TwoLevelPartitionGroupManager].l0Path(
      dataRepo, job, ctx, ".filter")
    println(s"Saving $fPath")
    filter.saveAsSequenceFile(fPath)
  }

  def loadInc1(env: Env) = {
    val k24 = new FilterTuple(0, 4, "k2")
    val k15 = new FilterTuple(0, 5, "k1")
    val keys = env.sparkCtx.makeRDD(List(k24, k15)).map(k => (NullWritable.get(), k))

    val mgr = PartitionGroupManager(env.sparkCtx.getConf, dataRepo, job)
    val ctx = PartitionGroupContext(ctxId, bucket, env)

    val fPath = mgr.asInstanceOf[TwoLevelPartitionGroupManager].l0Path(dataRepo, job, ctx, ".filter")
    println(s"Saving $fPath")
    keys.saveAsSequenceFile(fPath)
  }

  //import com.talena.agents.couchbase.mstore._; val env = Driver.env(sc, sqlContext); val mgr = Driver.mgr(sc); val ctx = Driver.ctx(env)
  //val filters = mgr.compactFilters(ctx)
}