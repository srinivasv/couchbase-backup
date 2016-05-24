package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

sealed trait Type
case class Filters() extends Type
case class Mutations() extends Type
case class All() extends Type

sealed trait Source {
  def getSnapshotOrAbort(msg: String): String = this match {
    case Snapshot(s) => s
    case _ => throw new IllegalArgumentException(msg)
  }
}
case class Mainline() extends Source
case class Snapshot(s: String) extends Source

case class PGroup[A <: Type](dataRepo: String, job: String, bucket: String, id: String)

@annotation.implicitNotFound(msg = "${A} is not compactible")
trait Compactible[A <: Type] {
  def compact(pg: PGroup[A], src: Source, to: String): Runnable[Unit]
  def move(pg: PGroup[A], from: String): Runnable[Unit]
  def cleanup(pg: PGroup[A]): Runnable[Unit]
}

@annotation.implicitNotFound(msg = "${A} is not mappable")
trait Mappable[A <: Type, B <: Source] {
  def map[C: ClassTag](pg: PGroup[A], src: B, f: MutationTuple => C): Runnable[RDD[C]]
}

object implicits {
  def compact[A <: Type: Compactible](pg: PGroup[A], src: Source, to: String): Runnable[Unit] = {
    implicitly[Compactible[A]].compact(pg, src, to)
  }

  def move[A <: Type: Compactible](pg: PGroup[A], from: String): Runnable[Unit] = {
    implicitly[Compactible[A]].move(pg, from)
  }

  def cleanup[A <: Type: Compactible](pg: PGroup[A]): Runnable[Unit] = {
    implicitly[Compactible[A]].cleanup(pg)
  }

  def map[A <: Type, B <: Source, C: ClassTag](pg: PGroup[A], src: B, f: MutationTuple => C)(implicit ev: Mappable[A, B])
  : Runnable[RDD[C]] = {
    ev.map[C](pg, src, f)
  }
}
