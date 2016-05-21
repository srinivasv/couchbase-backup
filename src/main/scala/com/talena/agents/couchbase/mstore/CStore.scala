package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.mstore.Utils

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{Text, NullWritable}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag

object CStore extends LazyLogging {
  @annotation.implicitNotFound(msg = "${A} is not deduplicable")
  trait Deduplicable[A] {
    def deduplicate(a: RDD[A]): State[RDD[A]]
  }

  def deduplicate[A: Deduplicable](a: RDD[A]): State[RDD[A]] = {
    implicitly[Deduplicable[A]].deduplicate(a)
  }

  implicit object FilterDeduplicator extends Deduplicable[FilterTuple] {
    override def deduplicate(rdd: RDD[FilterTuple]): State[RDD[FilterTuple]] = {
      State(rdd
        .map(f => (f.key(), f))
          .reduceByKey((f1, f2) => { if(f1.seqNo > f2.seqNo) f1 else f2 })
          .map({ case (k, f) => f }))
    }
  }

  @annotation.implicitNotFound(msg = "${A} is not broadcastable")
  trait Broadcastable[A, B] {
    def broadcast(a: RDD[A]): State[Broadcast[B]]
  }

  def broadcast[A, B](a: RDD[A])(implicit ev: Broadcastable[A, B]): State[Broadcast[B]] = {
    ev.broadcast(a)
  }

  implicit object FilterKeysBroadcaster extends Broadcastable[FilterTuple, Set[String]] {
      override def broadcast(rdd: RDD[FilterTuple]): State[Broadcast[Set[String]]] = {
      State(rdd.sparkContext.broadcast(rdd
        .map(f => f.key())
        .collect()
        .toSet))
    }
  }

  implicit object FilterSeqnoTuplesBroadcaster
  extends Broadcastable[FilterTuple, Set[(Short, Long, Long)]] {
    override def broadcast(rdd: RDD[FilterTuple]): State[Broadcast[Set[(Short, Long, Long)]]] = {
      State(rdd.sparkContext.broadcast(rdd
        .map(f => (f.partitionId(), f.uuid(), f.seqNo()))
        .collect()
        .toSet))
    }
  }

  implicit object RBLogBroadcaster extends Broadcastable[RBLogTuple, Map[(Short, Long), Long]] {
    override def broadcast(rdd: RDD[RBLogTuple]): State[Broadcast[Map[(Short, Long), Long]]] = {
      State(rdd.sparkContext.broadcast(rdd
        .map(r => ((r.partition(), r.uuid()), r.seqno()))
        .collectAsMap()))
    }
  }

  def openMutations(path: String): Environment[Option[RDD[MutationTuple]]] = {
    Environment(env => {
      def open(files: Array[FileStatus]): RDD[MutationTuple] = {
        env.sc.sequenceFile[NullWritable, MutationTuple](path).map({ case (_, v) => v })
      }

      val fs = FileSystem.get(env.sc.hadoopConfiguration)
      Utils.listFiles(fs, new Path(path)).map(open)
    })
  }

  def openFilter(path: String): Environment[Option[RDD[FilterTuple]]] = {
    Environment(env => {
      def open(files: Array[FileStatus]): RDD[FilterTuple] = {
        env.sc.sequenceFile[NullWritable, FilterTuple](path).map({ case (_, v) => v })
      }

      val fs = FileSystem.get(env.sc.hadoopConfiguration)
      Utils.listFiles(fs, new Path(path)).map(open)
    })
  }

  def persist(rdd: RDD[FilterTuple], path: String): Unit = {
    rdd.map(t => (NullWritable.get(), t)).saveAsSequenceFile(path)
  }

  def openRBLog(path: String): Environment[Option[RDD[RBLogTuple]]] = {
    Environment(env => {
      def open(files: Array[FileStatus]): RDD[RBLogTuple] = {
        env.sc.sequenceFile[NullWritable, RBLogTuple](path).map({ case (_, v) => v })
      }

      val fs = FileSystem.get(env.sc.hadoopConfiguration)
      Utils.listFiles(fs, new Path(path)).map(open)
    })
  }
}

case class State[A](a: A) {
  def get() = a
  def map[B](f: A => B): State[B] = State(f(a))
  def flatMap[B](f: A => State[B]): State[B] = f(a)
  def withFilter[B](p: A => B): State[B] = State(p(a))
}

case class Environment[A](g: Env => A) {
  def apply(e: Env): A = g(e)
  def map[B](f: A => B): Environment[B] = Environment(e => f(g(e)))
  def flatMap[B](f: A => Environment[B]): Environment[B] = Environment(e => f(g(e))(e))
}

case class Env(sc: SparkContext)
