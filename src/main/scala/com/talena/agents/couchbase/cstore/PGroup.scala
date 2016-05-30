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

case class PGroup[A <: Type](bucket: String, id: String)

@annotation.implicitNotFound(msg = "${A} is not compactible")
trait Compactible[A <: Type] {
  def compact(pg: PGroup[A], from: String, to: String): Runnable[Unit]
  def move(pg: PGroup[A], from: String, to: String): Runnable[Unit]
}

@annotation.implicitNotFound(msg = "${A} is not readable")
trait Readable[A <: Type, B] {
  def read(pg: PGroup[A], from: String): Runnable[RDD[B]]
}

object implicits {
  def compact[A <: Type: Compactible](pg: PGroup[A], from: String, to: String): Runnable[Unit] = {
    implicitly[Compactible[A]].compact(pg, from, to)
  }

  def move[A <: Type: Compactible](pg: PGroup[A], from: String, to: String): Runnable[Unit] = {
    implicitly[Compactible[A]].move(pg, from, to)
  }

  def read[A <: Type, B](pg: PGroup[A], from: String)(implicit ev: Readable[A, B])
  : Runnable[RDD[B]] = {
    ev.read(pg, from)
  }
}
