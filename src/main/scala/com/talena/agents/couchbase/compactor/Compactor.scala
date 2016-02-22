package com.talena.agents.couchbase.compactor

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Failure
import scala.util.Success
import scala.util.Try

abstract class Stream(mutations: Option[DataFrame], filter: Option[DataFrame])
abstract class GenerationalStream(
  mutations: Option[DataFrame], filter: Option[DataFrame])
  extends Stream(mutations, filter)
case class PreGenerationalStream(
  mutations: Option[DataFrame], filter: Option[DataFrame])
  extends Stream(mutations, filter)
case class YoungGenerationStream(
  mutations: Option[DataFrame], filter: Option[DataFrame])
  extends GenerationalStream(mutations, filter)
case class OldGenerationStream(
  mutations: Option[DataFrame], filter: Option[DataFrame])
  extends GenerationalStream(mutations, filter)

case class Filter(pid: Int, seqno: Long, key: String)

object Compactor extends App {
  def logError(msg: String) = {
    System.err.println(msg)
  }

  def haltWithError(msg: String) = {
    logError(msg)
    System.exit(1)
  }

  def parseJson(file: String) = {
    val jsonRow = sqlCtx.read.json(file).first()
    def indexOf(col: String) = jsonRow.fieldIndex(col)
    List("preGenDir", "youngGenDir", "oldGenDir")
      .map(d => (d, jsonRow(indexOf(d)).toString()))
  }

  def parseArgs() = {
    args match {
      case Array(file) => Some(parseJson(file))
      case _ => None
    }
  }

  def initStream(stream: (String, String)) = {
    import sqlCtx.implicits._
    println("Initializing stream: " + stream)
    val mutationsFiles = stream._2 + "/mutations-*"
    val filterFile = stream._2 + "/filter"
    val mutations = Try(sqlCtx.read.text(mutationsFiles)) match {
      case Success(m) => Some(m)
      case Failure(e) => logError(e.toString()); None
    }
    val filter = Try(sqlCtx.read.text(filterFile)) match {
      case Success(f) => Some(f
        .map(_(0).toString().split(","))
        .map(t => Filter(t(0).toInt, t(1).toLong, t(2)))
        .toDF()
      )
      case Failure(e) => logError(e.toString()); None
    }
    stream._1 match {
      case "preGenDir" => PreGenerationalStream(mutations, filter)
      case "youngGenDir" => YoungGenerationStream(mutations, filter)
      case "oldGenDir" => OldGenerationStream(mutations, filter)
    }
  }

  def initStreams() = {
    parseArgs() match {
      case Some(l) => l.map(initStream)
      case None => haltWithError("JSON file path argument missing")
    }
  }

  def removeDuplicatesInFilter(stream: Stream) = {
    import sqlCtx.implicits._
    stream match {
      case PreGenerationalStream(_, Some(filter)) => filter
        .groupBy($"pid", $"key")
        .max("seqno")
        .toDF("pid", "key", "seqno")
    }
  }

  def compact() = {
    val newFilter = removeDuplicatesInFilter(preGenStream)
    newFilter.explain()
    newFilter.show()
  }

  val conf = new SparkConf().setAppName("Couchbase Compactor")
  val sparkCtx = new SparkContext(conf)
  val sqlCtx = new org.apache.spark.sql.SQLContext(sparkCtx)

  val (preGenStream, youngGenStream, oldGenStream) = initStreams() match {
    case List(p: Stream, y: Stream, o: Stream) => (p, y, o)
  }
  compact()

  sparkCtx.stop()
}
