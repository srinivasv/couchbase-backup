package com.talena.agents.couchbase.compactor

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Mutation(pid: Int, seqno: Long, key: String, value: String,
  meta: String, data: String)
case class Filter(pid: Int, seqno: Long, key: String)

abstract class File
case object MutationFile extends File
case object FilterFile extends File

object CompactorApp extends App {
  def logError(msg: String) = {
    System.err.println(msg)
  }

  def parseJson(file: String) = {
    import sqlCtx.implicits._
    val spec = sqlCtx.read.json(file)
    def v(row: Row, col: String) = row(row.fieldIndex(col))

    val l0Row = spec.select("pset.id", "pset.l0.location", "pset.l0.format")
      .collect()(0)
    val l0Props = MutationStoreProps(v(l0Row, "id").toString(), L0,
      v(l0Row, "location").toString(), CSV)

    val l1Row = spec.select("pset.id", "pset.l1.location", "pset.l1.format")
      .collect()(0)
    val l1Props = MutationStoreProps(v(l1Row, "id").toString(), L1,
      v(l1Row, "location").toString(), CSV)

    val l2Row = spec.select("pset.id", "pset.l2.location", "pset.l2.format")
      .collect()(0)
    val l2Props = MutationStoreProps(v(l2Row, "id").toString(), L2,
      v(l2Row, "location").toString(), CSV)

    List(l0Props, l1Props, l2Props)
  }

  def parseArgs() = {
    args match {
      case Array(file) => Some(parseJson(file))
      case _ => None
    }
  }

  def verifyLocation(fs: FileSystem, location: String) = {
    val path = new Path(location)
    if (!fs.exists(path))
      throw new IllegalArgumentException(location + " does not exist")
    if (!fs.getFileStatus(path).isDirectory)
      throw new IllegalArgumentException(location + " is not a directory")
    true
  }

  def verifyFiles(status: Boolean, fs: FileSystem, mutationFiles: String, filterFile: String) = {
    val mutationsFilesExist = fs.exists(new Path(mutationFiles))
    val filterFileExists = fs.exists(new Path(filterFile))
    (mutationsFilesExist, filterFileExists) match {
      case (true, true) => true
      case (false, false) => false
      case (true, false) => throw new IllegalArgumentException(
        "Filter file " + filterFile + " missing")
      case (false, true) => throw new IllegalArgumentException(
        "Mutation files " + mutationFiles + " missing")
    }
  }

  def openDataFrame(files: String, fileType: File) = {
    import sqlCtx.implicits._
    val df = sparkCtx.textFile(files)
    fileType match {
      case MutationFile => df.map(_.split(",")).map(p => Mutation(
        p(0).toInt, p(1).toInt, p(2), p(3), p(4), p(5))).toDF()
      case FilterFile => df.map(_.split(",")).map(p => Filter(
        p(0).toInt, p(1).toInt, p(2))).toDF()
    }
  }

  def openDataFramePair(status: Boolean, mutationFiles: String,
    filterFile: String, format: FileFormat) = {

    status match {
     case true => Some(DataFramePair(
      openDataFrame(mutationFiles, MutationFile),
      openDataFrame(filterFile, FilterFile)))
     case false => None
    }
  }

  def openMutationStore(props: MutationStoreProps) = {
    println("Initializing props: " + props)
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val mutationFiles = props.location + "*.db"
    val filterFile = props.location + "*.meta"
    val dataFramePair = Try(verifyLocation(fs, props.location))
      .map(status => verifyFiles(status, fs, mutationFiles, filterFile))
      .map(status => openDataFramePair(status, mutationFiles, filterFile,
        props.format))
    (props.level, dataFramePair) match {
      case (L0, Success(Some(pair))) => L0(pair, props)
      case (L0, Success(None)) => throw new IllegalArgumentException(
        "L0 location may not be empty")
      case (L1, Success(Some(pair))) => L1(Some(pair), props)
      case (L1, Success(None)) => L1(None, props)
      case (L2, Success(Some(pair))) => L2(Some(pair), props)
      case (L2, Success(None)) => L2(None, props)
      case (_, Failure(ex)) => ex
    }
  }

  def openMutationStores() = {
    parseArgs() match {
      case Some(l) => l.map(openMutationStore)
      case None => throw new IllegalArgumentException(
        "JSON file path argument missing")
    }
  }

  val conf = new SparkConf().setAppName("Couchbase Compactor")
  val sparkCtx = new SparkContext(conf)
  val sqlCtx = new SQLContext(sparkCtx)

  val (l0: L0, l1: L1, l2: L2) = openMutationStores() match {
    case List(l0: L0, l1: L1, l2: L2) => (l0, l1, l2)
  }

  val compactor = new Compactor(sqlCtx, l0, l1, l2)
  compactor.compact()

  sparkCtx.stop()
}
