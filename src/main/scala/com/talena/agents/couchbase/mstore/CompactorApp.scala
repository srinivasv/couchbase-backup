package com.talena.agents.couchbase.mstore

import org.apache.hadoop.fs.{FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object CompactorApp extends App {
  private def logError(msg: String): Unit = {
    System.err.println(msg)
  }

  private def parseJson(file: String): (Props, Props, Props) = {
    import env.sqlCtx.implicits._
    val props = env.sqlCtx.read.json(file)
    def v(row: Row, col: String) = row(row.fieldIndex(col))

    val l0Row = props.select("pset.id", "pset.l0.location", "pset.l0.format")
      .collect()(0)
    val l0Props = Props(v(l0Row, "id").toString(), Level0,
      v(l0Row, "location").toString(), CSV, env)

    val l1Row = props.select("pset.id", "pset.l1.location", "pset.l1.format")
      .collect()(0)
    val l1Props = Props(v(l1Row, "id").toString(), Level1,
      v(l1Row, "location").toString(), CSV, env)

    val l2Row = props.select("pset.id", "pset.l2.location", "pset.l2.format")
      .collect()(0)
    val l2Props = Props(v(l2Row, "id").toString(), Level2,
      v(l2Row, "location").toString(), CSV, env)

    (l0Props, l1Props, l2Props)
  }

  private def parseArgs(): Option[(Props, Props, Props)] = {
    args match {
      case Array(file) => Some(parseJson(file))
      case _ => None
    }
  }

  private def compact() = {
    l0.dedupeFilter()
  }

  private def newEnv(): Env = {
    val conf = new SparkConf().setAppName("Couchbase Compactor")
    val sparkCtx = new SparkContext(conf)
    val sqlCtx = new SQLContext(sparkCtx)
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    Env(sparkCtx, sqlCtx, fs)
  }

  val env = newEnv()
  val (l0, l1, l2) = parseArgs() match {
    case Some(propsTuple) => MStore.open(propsTuple)
    case None => throw new IllegalArgumentException(
      "JSON file path argument missing")
  }

  compact()

  env.sparkCtx.stop()
}
