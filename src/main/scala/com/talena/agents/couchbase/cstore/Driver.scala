package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}

import org.apache.hadoop.fs.{FileSystem}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

object Driver extends LazyLogging {
  val repo = "Users/srinivas/tmp/cstore/prod"
  val job = "sales"
  val bucket = "default"
  val props = List(BucketProps(bucket, 1))
  val temp = "Users/srinivas/tmp/cstore/tmp"

  val pgid = "1"

  def run(sc: SparkContext) = {
    val env = getEnv(sc)

    runIter(env, "Iteration 1", i1)
    //runIter(env, "Iteration 2", i2)
  }

  def runIter(env: Env, msg: String, iter: Env => Unit) = {
    logger.info(s"Running: $msg")
    iter(env)
    logger.info("Before runnable compact filters")
    val r = RunnableCStore.compactFilters(repo, job, props, temp)
    logger.info("Before running compact filters")
    r(env)
    logger.info("After running compact filters")
    // Snapshot
    // CStore.compactMutations(repo, job, props, temp)
  }

  def i1(env: Env) = {
    val f1 = new FilterTuple(0, 1, "k1")
    val f2 = new FilterTuple(0, 2, "k2")
    val f3 = new FilterTuple(0, 3, "k1")

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)
    val m3 = new MutationTuple(f3)

    env.sparkCtx.makeRDD(List(f1, f2, f3))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(fpath)

    env.sparkCtx.makeRDD(List(m1, m2, m3))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(mpath)
  }

  def i2(env: Env) = {
    val f1 = new FilterTuple(0, 4, "k2")
    val f2 = new FilterTuple(0, 5, "k1")

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)

    env.sparkCtx.makeRDD(List(f1, f2))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(fpath)

    env.sparkCtx.makeRDD(List(m1, m2))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(mpath)
  }

  def getEnv(sc: SparkContext) = Env(sc.getConf, sc, new SQLContext(sc),
    FileSystem.get(sc.hadoopConfiguration))

  def path = List(repo, job, bucket, "l0", pgid).mkString("/", "/", "")
  def mpath = path + ".mutations"
  def fpath = path + ".filter"
  def rpath = path + ".rblog"
}