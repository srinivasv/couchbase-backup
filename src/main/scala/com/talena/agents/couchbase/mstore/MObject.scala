package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.Map
import scala.reflect.ClassTag

case class MState[A](a: A) {
  def map[B](f: A => B): MState[B] = MState(f(a))
  def flatMap[B](f: A => MState[B]): MState[B] = f(a)
  def withFilter[B](p: A => B): MState[B] = MState(p(a))
}

@annotation.implicitNotFound(msg = "${A} is not deduplicable")
trait Deduplicable[A] {
  def deduplicate(a: RDD[A]): MState[RDD[A]]
}

@annotation.implicitNotFound(msg = "${A} is not broadcastable")
trait Broadcastable[A, B] {
  def broadcast(a: RDD[A]): MState[Broadcast[B]]
}

object Transformations {
  def deduplicate[A: Deduplicable](a: RDD[A]): MState[RDD[A]] = {
    implicitly[Deduplicable[A]].deduplicate(a)
  }

  def broadcast[A, B](a: RDD[A])(implicit ev: Broadcastable[A, B]): MState[Broadcast[B]] = {
    //implicitly[Broadcastable[A, B]].broadcast(a)
    ev.broadcast(a)
  }
}

object Test {
  def test(env: Env) = {
    val f1 = new FilterTuple(0, 1, "k1")
    val f2 = new FilterTuple(0, 2, "k2")
    val f3 = new FilterTuple(0, 3, "k1")
    val fRDD = env.sparkCtx.makeRDD(List(f1, f2, f3))

    val m1 = new MutationTuple(f1)
    val m2 = new MutationTuple(f2)
    val m3 = new MutationTuple(f3)
    val mRDD = env.sparkCtx.makeRDD(List(m1, m2, m3))

    val rRDD = env.sparkCtx.makeRDD(List(new RBLogTuple(0, 11111, 10)))

    import Transformations.{broadcast, deduplicate}
    import Deduplicators.FilterDeduplicator
    import Broadcasters.{FilterSeqnoTuplesBroadcaster, RBLogBroadcaster}

    for {
      r <- MState(rRDD)
      r1 <- broadcast(r)
      f <- MState(fRDD) if f.filter(t => t.seqNo() < r1.value.getOrElse((t.partitionId(), t.uuid()), (t.seqNo + 1)))
      d <- deduplicate(f)
      f1 <- broadcast(d)
    } yield(f1)
  }
}
