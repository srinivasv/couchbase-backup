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

sealed trait Source
case class Mainline() extends Source
case class Snapshot(id: String) extends Source

case class PGroup[A <: Type](dataRepo: String, job: String, bucket: String, id: String)

@annotation.implicitNotFound(msg = "Combination (${A}, ${B}) is not compactible")
trait Compactible[A <: Type, B <: Source] {
  def compact(pg: PGroup[A], src: B, to: String): Runnable[Unit]
  def move(pg: PGroup[A], from: String): Runnable[Unit]
  def cleanup(pg: PGroup[A]): Runnable[Unit]
}

@annotation.implicitNotFound(msg = "Combination (${A}, ${B}) is not mappable")
trait Mappable[A <: Type, B <: Source] {
  def map[C: ClassTag](pg: PGroup[A], src: B, f: MutationTuple => C)
  : Runnable[Transformable[RDD[C]]]
}

object implicits {
  def compact[A <: Type, B <: Source](pg: PGroup[A], src: B, to: String)
      (implicit ev: Compactible[A, B]): Runnable[Unit] = {
    ev.compact(pg, src, to)
  }

  def move[A <: Type, B <: Source](pg: PGroup[A], from: String)(implicit ev: Compactible[A, B])
  : Runnable[Unit] = {
    ev.move(pg, from)
  }

  def cleanup[A <: Type, B <: Source](pg: PGroup[A])(implicit ev: Compactible[A, B])
  : Runnable[Unit] = {
    ev.cleanup(pg)
  }

  def map[A <: Type, B <: Source, C: ClassTag](pg: PGroup[A], src: B, f: MutationTuple => C)
      (implicit ev: Mappable[A, B]): Runnable[Transformable[RDD[C]]] = {
    ev.map[C](pg, src, f)
  }
}
