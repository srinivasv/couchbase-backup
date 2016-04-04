package com.talena.agents.couchbase.mstore

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import com.talena.agents.couchbase.core.CouchbaseLongRecord;
import com.talena.agents.couchbase.core.CouchbaseShortRecord;

sealed trait MStore {
  def dedupeFilter(): MStore = {
    this match {
      case L0(MData(mutations, Filter(dataFrame, source), rblog), props) =>
        import props.env.sqlCtx.implicits._
        L0(MData(mutations, Filter(dataFrame
            .groupBy($"pid", $"key")
            .max("seqno")
            .toDF("pid", "key", "seqno"), source),
          rblog), props)
      case _ => this
    }
  }

  def updateFilter(other: MStore): MStore = {
    (this, other) match {
      case (L1(Some(MData(mutations, ourFilter, rblog)), props),
            L0(MData(_, otherFilter, _), _))  =>
        L1(Some(MData(mutations,
          updateFilter(ourFilter, otherFilter, props.env.sqlCtx),
          rblog)), props)
      case (L2(Some(MData(mutations, ourFilter, rblog)), props),
            L0(MData(_, otherFilter, _), _))  =>
        L2(Some(MData(mutations,
          updateFilter(ourFilter, otherFilter, props.env.sqlCtx),
          rblog)), props)
      case (_, _) => this
    }
  }

  def applyFilter(): MStore = {
    this
  }

  private def updateFilter(our: Filter, other: Filter, sqlCtx: SQLContext)
    : Filter = {
    import sqlCtx.implicits._
    our.dataFrame.registerTempTable("our")
    other.dataFrame.registerTempTable("other")
    val df = sqlCtx.sql("SELECT NVL(our.pid, other.pid) pid, " +
      "NVL(our.key, other.key) key, NVL(our.seqno, other.seqno) seqno " +
      "FROM other LEFT JOIN our ON other.key = our.key")

    /*
    val ourDF = our.dataFrame.toDF("ourpid", "ourkey", "ourseqno")
    val otherDF = other.dataFrame.toDF("theirpid", "theirkey", "theirseqno")
    val updatedDF = ourDF.join(theirDF, $"ourkey" === $"otherkey", "left_outer")
    */
    our
  }
}

case class L0(data: MData, props: Props) extends MStore
case class L1(data: Option[MData], props: Props) extends MStore
case class L2(data: Option[MData], props: Props) extends MStore

case class MData(mutations: Mutations, filter: Filter, rblog: Option[RBLog])

sealed trait MDataClass
case class Mutations(dataFrame: DataFrame, source: Files) extends MDataClass
case class Filter(dataFrame: DataFrame, source: File) extends MDataClass
case class RBLog(dataFrame: DataFrame, source: File) extends MDataClass

sealed trait DataFrameStorage
case class File(file: String, format: FileFormat) extends DataFrameStorage
case class Files(files: Array[String], format: FileFormat)
  extends DataFrameStorage

sealed trait FileFormat
case object CSV extends FileFormat
case object JSON extends FileFormat
case object SequenceFile extends FileFormat

case class Props(id: String, level: Level, loc: String, format: FileFormat,
  env: Env)

case class Env(sparkCtx: SparkContext, sqlCtx: SQLContext, fs: FileSystem)

sealed trait Level
case object Level0 extends Level
case object Level1 extends Level
case object Level2 extends Level

object MStore {
  def open(props:(Props, Props, Props)): (MStore, MStore, MStore) = {
    val (props0, props1, props2) = props
    (openMStore(props0), openMStore(props1), openMStore(props2))
  }

  private def openMStore(props: Props): MStore = {
    (props.level, openMData(props))  match {
      case (Level0, Some(mdata)) => L0(mdata, props)
      case (Level0, None) => throw new IllegalArgumentException(
        "L0 location may not be empty");
      case (Level1, mdata) => L1(mdata, props)
      case (Level2, mdata) => L2(mdata, props)
    }
  }

  private def openMData(props: Props): Option[MData] = {
    val mutationFiles = props.loc + "/*.db"
    val filterFile = props.loc + "/*.meta"
    if(verifyFiles(props.env.fs, props.loc, (mutationFiles, filterFile))) {
      Some(MData(
        openMutations(mutationFiles, props),
        openFilter(filterFile, props),
        openRBLog(props)))
    } else {
      None
    }
  }

  private def openMutations(files: String, props: Props): Mutations = {
    import props.env.sqlCtx.implicits._
    //val df = props.env.sparkCtx.sequenceFile[CouchbaseShortRecord,
    //  CouchbaseLongRecord](files).toDF()
    val df = props.env.sparkCtx.sequenceFile[Text, CouchbaseShortRecord](
      files)
      .map(r => (r._2.partitionId(), r._1.toString(), r._2.seqNo()))
      .toDF("pid", "key", "seqno")
    Mutations(df, Files(Array(files), props.format))
  }

  private def openFilter(file: String, props: Props): Filter = {
    import props.env.sqlCtx.implicits._
    val df = props.env.sparkCtx.sequenceFile[Text, CouchbaseShortRecord](file)
      .map(r => (r._2.partitionId(), r._1.toString(), r._2.seqNo()))
      .toDF("pid", "key", "seqno")
    Filter(df, File(file, props.format))
  }

  private def openRBLog(props: Props): Option[RBLog] = {
    None
  }

  private def verifyFiles(fs: FileSystem, loc: String, files: (String, String))
    : Boolean = {
    Utils.verifyLocation(fs, loc)
    val (mutationFiles, filterFile) = files
    val mutationsFilesExist = (fs.globStatus(new Path(mutationFiles))
      .length > 0)
    val filterFileExists = (fs.globStatus(new Path(filterFile)).length == 1)
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
}
