package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class PGroup[A](bucket: String, id: String)

@annotation.implicitNotFound(msg = "${A} is not compactible")
trait Compactible[A] {
  def needsCompaction(pg: PGroup[A], home: String): Runnable[Boolean]
  def compactAndPersist(pg: PGroup[A], home: String, temp: String): Runnable[Unit]
  def moveAndCleanup(pg: PGroup[A], temp: String, home: String): Runnable[Unit]
}

@annotation.implicitNotFound(msg = "${A} is not readable")
trait Readable[A] {
  def read(pg: PGroup[A], home: String): Runnable[RDD[A]]
}

object implicits {
  def needsCompaction[A: Compactible](pg: PGroup[A], home: String): Runnable[Boolean] = {
    implicitly[Compactible[A]].needsCompaction(pg, home)
  }

  def compactAndPersist[A: Compactible](pg: PGroup[A], home: String, temp: String)
  : Runnable[Unit] = {
    implicitly[Compactible[A]].compactAndPersist(pg, home, temp)
  }

  def moveAndCleanup[A: Compactible](pg: PGroup[A], temp: String, home: String): Runnable[Unit] = {
    implicitly[Compactible[A]].moveAndCleanup(pg, temp, home)
  }

  def read[A: Readable](pg: PGroup[A], home: String): Runnable[RDD[A]] = {
    implicitly[Readable[A]].read(pg, home)
  }
}
