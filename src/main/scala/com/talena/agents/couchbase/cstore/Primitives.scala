package com.talena.agents.couchbase.cstore

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.NullWritable

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map

case class Transformable[A](a: A) {
  def get() = a
  def map[B](f: A => B): Transformable[B] = Transformable(f(a))
  def flatMap[B](f: A => Transformable[B]): Transformable[B] = f(a)
  def withFilter[B](p: A => B): Transformable[B] = Transformable(p(a))
}

case class Runnable[A](g: Env => A) {
  def apply(e: Env): A = g(e)
  def map[B](f: A => B): Runnable[B] = Runnable(e => f(g(e)))
  def flatMap[B](f: A => Runnable[B]): Runnable[B] = Runnable(e => f(g(e))(e))
}

case class Env(sc: SparkContext)

@annotation.implicitNotFound(msg = "${A} is not readable")
trait Readable[A] {
  def read(path: String): Runnable[Option[RDD[A]]]
  def readOrGetEmpty(path: String): Runnable[RDD[A]]
  def readOrAbort(path: String, msg: String): Runnable[RDD[A]]
}

@annotation.implicitNotFound(msg = "${A} is not writable")
trait Writable[A] {
  def write(a: RDD[A], path: String): Runnable[Unit]
}

@annotation.implicitNotFound(msg = "${A} is not deduplicable")
trait Deduplicable[A] {
  def deduplicate(a: RDD[A]): Transformable[RDD[A]]
}

@annotation.implicitNotFound(msg = "${A} is not broadcastable")
trait Broadcastable[A, B] {
  def broadcast(a: RDD[A]): Transformable[Broadcast[B]]
}

object primitives extends LazyLogging {
  object implicits {
    def read[A: Readable](path: String): Runnable[Option[RDD[A]]] =
      implicitly[Readable[A]].read(path)

    def readOrGetEmpty[A: Readable](path: String): Runnable[RDD[A]] =
      implicitly[Readable[A]].readOrGetEmpty(path)

    def readOrAbort[A: Readable](path: String, msg: String): Runnable[RDD[A]] =
      implicitly[Readable[A]].readOrAbort(path, msg)

    def write[A: Writable](a: RDD[A], path: String): Runnable[Unit] =
      implicitly[Writable[A]].write(a, path)

    def deduplicate[A: Deduplicable](a: RDD[A]): Transformable[RDD[A]] =
      implicitly[Deduplicable[A]].deduplicate(a)

    def broadcast[A, B](a: RDD[A])(implicit ev: Broadcastable[A, B])
    : Transformable[Broadcast[B]] = ev.broadcast(a)
  }

  implicit object MutationsReaderWriter extends Readable[MutationTuple]
  with Writable[MutationTuple] {
    override def read(path: String): Runnable[Option[RDD[MutationTuple]]] = {
      Runnable(env => {
        val fullPath = path + CStoreProps.MutationsFileExtension(env.sc.getConf)
        val fs = FileSystem.get(env.sc.hadoopConfiguration)
        Utils.listFiles(fs, new Path(fullPath))
          .map(_ =>
            env.sc.sequenceFile[NullWritable, MutationTuple](fullPath).map({ case (_, v) => v }))
      })
    }

    override def readOrGetEmpty(path: String): Runnable[RDD[MutationTuple]] = {
      Runnable(env => env.sc.emptyRDD[MutationTuple])
    }

    override def readOrAbort(path: String, msg: String): Runnable[RDD[MutationTuple]] = {
      _readOrAbort[MutationTuple](read(path), s"$msg: $path")
    }

    override def write(a: RDD[MutationTuple], path: String): Runnable[Unit] = {
      Runnable(_ => a.map(m => (NullWritable.get(), m)).saveAsSequenceFile(path))
    }
  }

  implicit object FilterReaderWriter extends Readable[FilterTuple] with Writable[FilterTuple] {
    override def read(path: String): Runnable[Option[RDD[FilterTuple]]] = {
      Runnable(env => {
        val fullPath = path + CStoreProps.FilterFileExtension(env.sc.getConf)
        val fs = FileSystem.get(env.sc.hadoopConfiguration)
        Utils.listFiles(fs, new Path(fullPath))
          .map(_ =>
            env.sc.sequenceFile[NullWritable, FilterTuple](fullPath).map({ case (_, v) => v }))
      })
    }

    override def readOrGetEmpty(path: String): Runnable[RDD[FilterTuple]] = {
      Runnable(env => env.sc.emptyRDD[FilterTuple])
    }

    override def readOrAbort(path: String, msg: String): Runnable[RDD[FilterTuple]] = {
      _readOrAbort[FilterTuple](read(path), s"$msg: $path")
    }

    override def write(a: RDD[FilterTuple], path: String): Runnable[Unit] = {
      Runnable(_ => a.map(m => (NullWritable.get(), m)).saveAsSequenceFile(path))
    }
  }

  implicit object RBLogReader extends Readable[RBLogTuple] {
    override def read(path: String): Runnable[Option[RDD[RBLogTuple]]] = {
      Runnable(env => {
        val fullPath = path + CStoreProps.RBLogFileExtension(env.sc.getConf)
        val fs = FileSystem.get(env.sc.hadoopConfiguration)
        Utils.listFiles(fs, new Path(fullPath))
          .map(_ =>
            env.sc.sequenceFile[NullWritable, RBLogTuple](fullPath).map({ case (_, v) => v }))
      })
    }

    override def readOrGetEmpty(path: String): Runnable[RDD[RBLogTuple]] = {
      Runnable(env => env.sc.emptyRDD[RBLogTuple])
    }

    override def readOrAbort(path: String, msg: String): Runnable[RDD[RBLogTuple]] = {
      _readOrAbort[RBLogTuple](read(path), s"$msg: $path")
    }
  }

  implicit object FilterDeduplicator extends Deduplicable[FilterTuple] {
    override def deduplicate(rdd: RDD[FilterTuple]): Transformable[RDD[FilterTuple]] = {
      Transformable(rdd
        .map(f => (f.key(), f))
        .reduceByKey((f1, f2) => { if (f1.seqNo > f2.seqNo) f1 else f2 })
        .map({ case (k, f) => f }))
    }
  }

  implicit object FilterKeysBroadcaster extends Broadcastable[FilterTuple, Set[String]] {
      override def broadcast(rdd: RDD[FilterTuple]): Transformable[Broadcast[Set[String]]] = {
      Transformable(rdd.sparkContext.broadcast(rdd
        .map(f => f.key())
        .collect()
        .toSet))
    }
  }

  implicit object FilterSeqnoTuplesBroadcaster
  extends Broadcastable[FilterTuple, Set[(Short, Long, Long)]] {
    override def broadcast(rdd: RDD[FilterTuple])
    : Transformable[Broadcast[Set[(Short, Long, Long)]]] = {
      Transformable(rdd.sparkContext.broadcast(rdd
        .map(f => (f.partitionId(), f.uuid(), f.seqNo()))
        .collect()
        .toSet))
    }
  }

  implicit object RBLogBroadcaster extends Broadcastable[RBLogTuple, Map[(Short, Long), Long]] {
    override def broadcast(rdd: RDD[RBLogTuple])
    : Transformable[Broadcast[Map[(Short, Long), Long]]] = {
      Transformable(rdd.sparkContext.broadcast(rdd
        .map(r => ((r.partition(), r.uuid()), r.seqno()))
        .collectAsMap()))
    }
  }

  private def _readOrAbort[A](rOpt: Runnable[Option[RDD[A]]], msg: String): Runnable[RDD[A]] = {
    for {
      r0 <- rOpt
      r1 = r0 match {
        case Some(r) => r
        case None => throw new RuntimeException(s"$msg")
      }
    } yield(r1)
  }
}
