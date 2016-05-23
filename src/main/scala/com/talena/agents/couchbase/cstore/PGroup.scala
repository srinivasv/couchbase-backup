package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

sealed trait Type
case class Filters() extends Type
case class Mutations() extends Type
case class All() extends Type

sealed trait Source
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
trait Mappable[A <: Type] {
  def map[B](pg: PGroup[A], src: Source, f: MutationTuple => B): Runnable[RDD[B]]
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

  def map[A <: Type: Mappable, B](pg: PGroup[A], src: Source, f: MutationTuple => B)
  : Runnable[RDD[B]] = {
    implicitly[Mappable[A]].map(pg, src, f)
  }
}
