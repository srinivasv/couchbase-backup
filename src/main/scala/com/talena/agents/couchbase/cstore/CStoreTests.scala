package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}

import com.talena.agents.couchbase.cstore._
import com.talena.agents.couchbase.cstore.pgroup._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

object CStoreTests extends LazyLogging {
  def testFilterCompaction(sc: SparkContext) = {
    val env = getEnv(sc)
    setup(env)

    import primitives.FilterReaderWriter
    import primitives.implicits.read

    iter1(env)
    val cf1 = RunnableCStore.compactFilters(buckets, jobDir, tempDir)
    cf1(env)
    val f1RDD = read(l1.path(jobDir, pg, ".filter"))
    val f1 = f1RDD(env).collect().deep
    logger.info(s"Iteration 1: Compacted L1 filter: $f1")

    iter2(env)
    val cf2 = RunnableCStore.compactFilters(buckets, jobDir, tempDir)
    cf2(env)
    val f2RDD = read(l1.path(jobDir, pg, ".filter"))
    val f2 = f2RDD(env).collect().deep
    logger.info(s"Iteration 2: Compacted L1 filter: $f2")

    cleanup(env)
  }

  def testMutationsMapping(sc: SparkContext) = {
    case class Mapper() extends MutationsMapper[Unit] {
      def setup() = logger.info("Inside setup")
      def teardown() = logger.info("Inside teardown")
      def map(m: MutationTuple): Unit = logger.info(s"Inside map: $m")
    }

    val env = getEnv(sc)
    setup(env)

    runBackup(env, "Iteration 1", iter1)
    val r = RunnableCStore.mapMutations[Unit](buckets, jobDir, Mapper())
    r(env)

    cleanup(env)
  }

  def iter1(env: Env) = {
    val f1 = new FilterTuple(0, 1, "k1")
    val f2 = new FilterTuple(0, 2, "k2")
    val f3 = new FilterTuple(0, 3, "k1")

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)
    val m3 = new MutationTuple(f3)

    env.sparkCtx.makeRDD(List(f1, f2, f3))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(l0.path(jobDir, pg, ".filter"))

    env.sparkCtx.makeRDD(List(m1, m2, m3))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(l0.path(jobDir, pg, ".mutations"))
  }

  def iter2(env: Env) = {
    val f1 = new FilterTuple(0, 4, "k2")
    val f2 = new FilterTuple(0, 5, "k1")

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)

    env.sparkCtx.makeRDD(List(f1, f2))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(l0.path(jobDir, pg, ".filter"))

    env.sparkCtx.makeRDD(List(m1, m2))
      .map(k => (NullWritable.get(), k))
      .saveAsSequenceFile(l0.path(jobDir, pg, ".mutations"))
  }

  def runBackup(env: Env, msg: String, iter: Env => Unit) = {
    logger.info(s"Running: $msg")
    iter(env)
    val cf = RunnableCStore.compactFilters(buckets, jobDir, tempDir)
    cf(env)
  }

  def getEnv(sc: SparkContext) = Env(sc.getConf, sc, new SQLContext(sc),
    FileSystem.get(sc.hadoopConfiguration))

  def setup(env: Env) = {
    env.fs.mkdirs(new Path(cstoreDir))
    env.fs.mkdirs(new Path(jobDir))
    env.fs.mkdirs(new Path(bucketDir))
    env.fs.mkdirs(new Path(bucketDir + "/" + l0))
    env.fs.mkdirs(new Path(bucketDir + "/" + l1))
    env.fs.mkdirs(new Path(tempDir))
  }

  def cleanup(env: Env) = {
    env.fs.delete(new Path(cstoreDir), true)
  }

  val bucket = "default"
  val buckets = List(Bucket(bucket, 1))

  val homeDir = s"/Users/srinivas/tmp"
  val cstoreDir = s"$homeDir/cstore"
  val jobDir = s"$cstoreDir/prod"
  val bucketDir = s"$jobDir/$bucket"
  val tempDir = s"$cstoreDir/tmp"

  val pg = PGroup(bucket, "1")
}