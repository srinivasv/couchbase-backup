package com.talena.agents.couchbase.compactor

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CompactorApp extends App {
  def logError(msg: String): Unit = {
    System.err.println(msg)
  }

  def parseJson(file: String): List[LevelProps] = {
    import sqlContext.implicits._
    val props = sqlContext.read.json(file)
    def v(row: Row, col: String) = row(row.fieldIndex(col))

    val l0Row = props.select("pset.id", "pset.l0.location", "pset.l0.format")
      .collect()(0)
    val l0Props = LevelProps(v(l0Row, "id").toString(), L0,
      v(l0Row, "location").toString(), CSV)

    val l1Row = props.select("pset.id", "pset.l1.location", "pset.l1.format")
      .collect()(0)
    val l1Props = LevelProps(v(l1Row, "id").toString(), L1,
      v(l1Row, "location").toString(), CSV)

    val l2Row = props.select("pset.id", "pset.l2.location", "pset.l2.format")
      .collect()(0)
    val l2Props = LevelProps(v(l2Row, "id").toString(), L2,
      v(l2Row, "location").toString(), CSV)

    List(l0Props, l1Props, l2Props)
  }

  def parseArgs(): Option[List[LevelProps]] = {
    args match {
      case Array(file) => Some(parseJson(file))
      case _ => None
    }
  }

  def verifyLocation(location: String): Boolean = {
    val path = new Path(location)
    if (!fileSystem.exists(path))
      throw new IllegalArgumentException(location + " does not exist")
    if (!fileSystem.getFileStatus(path).isDirectory)
      throw new IllegalArgumentException(location + " is not a directory")
    true
  }

  def verifyFiles(location: String, files: (String, String)): Boolean = {
    verifyLocation(location)
    val (mutationFiles, filterFile) = files
    val mutationsFilesExist = (fileSystem.globStatus(new Path(mutationFiles))
      .length > 0)
    val filterFileExists = (fileSystem.globStatus(new Path(filterFile)).length
      == 1)
    (mutationsFilesExist, filterFileExists) match {
      case (true, true) =>
        System.out.println("Found " + files);
        true
      case (false, false) =>
        System.out.println(files + " missing");
        false
      case (true, false) => throw new IllegalArgumentException(
        "Filter file " + filterFile + " missing")
      case (false, true) => throw new IllegalArgumentException(
        "Mutation files " + mutationFiles + " missing")
    }
  }

  def getMutation(files: String, format: FileFormat): Mutation = {
    import sqlContext.implicits._
    val df = sparkContext.textFile(files)
      .map(_.split(",")).map(p => MutationSchema(
        p(0).toInt, p(1).toInt, p(2), p(3), p(4), p(5))).toDF()
    Mutation(df, Array(files), format)
  }

  def getFilter(file: String, format: FileFormat): Filter = {
    import sqlContext.implicits._
    val df = sparkContext.textFile(file)
      .map(_.split(",")).map(p => FilterSchema(
        p(0).toInt, p(1).toInt, p(2))).toDF()
    Filter(df, file, format)
  }

  def getFailoverLog(): Option[FailoverLog] = {
    None
  }

  def getMData(props: LevelProps): Option[MData] = {
    val mutationFiles = props.location + "/*.db"
    val filterFile = props.location + "/*.meta"
    if(verifyFiles(props.location, (mutationFiles, filterFile))) {
      Some(MData(
        getMutation(mutationFiles, props.format),
        getFilter(filterFile, props.format),
        getFailoverLog()))
    } else {
      None
    }
  }

  def openMStore(props: LevelProps): MStore = {
    println("Initializing props: " + props)
    (props.level, getMData(props))  match {
      case (L0, Some(mdata)) => L0(props.psid, props.location, mdata)
      case (L0, None) => throw new IllegalArgumentException(
        "L0 location may not be empty");
      case (L1, mdata) => L1(props.psid, props.location, mdata)
      case (L2, mdata) => L2(props.psid, props.location, mdata)
    }
  }

  def openMStores(): List[MStore] = {
    parseArgs() match {
      case Some(l) => l.map(openMStore)
      case None => throw new IllegalArgumentException(
        "JSON file path argument missing")
    }
  }

  val conf = new SparkConf().setAppName("Couchbase Compactor")
  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)
  val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

  val (l0: L0, l1: L1, l2: L2) = openMStores() match {
    case List(l0: L0, l1: L1, l2: L2) => (l0, l1, l2)
  }
  System.out.println(l0)
  System.out.println(l1)
  System.out.println(l2)

  val compactor = new Compactor(sqlContext, l0, l1, l2)
  compactor.compact()

  sparkContext.stop()
}
