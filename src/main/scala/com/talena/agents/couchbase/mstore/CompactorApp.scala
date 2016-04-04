package com.talena.agents.couchbase.mstore

import org.apache.hadoop.fs.{FileSystem}
import org.apache.hadoop.io.{IntWritable, Text, Writable}
import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

import java.io.{DataInput, DataOutput}

import com.talena.agents.couchbase.core.CouchbaseLongRecord;
import com.talena.agents.couchbase.core.CouchbaseShortRecord;

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
      v(l0Row, "location").toString(), SequenceFile, env)

    val l1Row = props.select("pset.id", "pset.l1.location", "pset.l1.format")
      .collect()(0)
    val l1Props = Props(v(l1Row, "id").toString(), Level1,
      v(l1Row, "location").toString(), SequenceFile, env)

    val l2Row = props.select("pset.id", "pset.l2.location", "pset.l2.format")
      .collect()(0)
    val l2Props = Props(v(l2Row, "id").toString(), Level2,
      v(l2Row, "location").toString(), SequenceFile, env)

    (l0Props, l1Props, l2Props)
  }

  private def parseArgs(): Option[(Props, Props, Props)] = {
    args match {
      case Array(file) => Some(parseJson(file))
      case _ => None
    }
  }

  private def data() = {
    val k11: CouchbaseShortRecord = new CouchbaseShortRecord(0, 1, "k1",
      CouchbaseShortRecord.Type.INSERT)
    val k22: CouchbaseShortRecord = new CouchbaseShortRecord(0, 2, "k2",
      CouchbaseShortRecord.Type.INSERT)
    val k13: CouchbaseShortRecord = new CouchbaseShortRecord(0, 3, "k1",
      CouchbaseShortRecord.Type.INSERT)
    val k34: CouchbaseShortRecord = new CouchbaseShortRecord(0, 4, "k3",
      CouchbaseShortRecord.Type.INSERT)
    val keys = env.sparkCtx.makeRDD(List(k11, k22, k13, k34)).map(
      k => (k.key(), k))

    keys.saveAsSequenceFile(
      "/Users/srinivas/workspace/couch/couchbase-backup/src/main/resources/" +
      "compaction/l0/filter.meta")
    /*
    val res = env.sparkCtx.sequenceFile[Text, CouchbaseShortRecord](
      "/Users/srinivas/workspace/couch/couchbase-backup/src/main/resources/" +
      "compaction/l0/filter.meta").map(x => (x._1.toString(), x._2.toString()))
    res.collect().map(x => System.out.println("dbg: " + x._1 + " " + x._2))
    */
  }

  private def compact() = {
    System.out.println("Before dedupe")
    l0.data.filter.dataFrame.explain()
    l0.data.filter.dataFrame.show()
    System.out.println("After dedupe")
    l0.dedupeFilter() match {
      case dedupedL0 @ L0(_, _) =>
        dedupedL0.data.filter.dataFrame.explain()
        dedupedL0.data.filter.dataFrame.show()
    }
  }

  private def newEnv(): Env = {
    val conf = new SparkConf().setAppName("Couchbase Compactor")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CouchbaseShortRecord]))

    val sparkCtx = new SparkContext(conf)
    val sqlCtx = new SQLContext(sparkCtx)
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    Env(sparkCtx, sqlCtx, fs)
  }

  val env = newEnv()
  System.out.println("Before parseArgs")
  val (l0: L0, l1: L1, l2: L2) = parseArgs() match {
    case Some(propsTuple) => MStore.open(propsTuple)
    case None => throw new IllegalArgumentException(
      "JSON file path argument missing")
  }

  //data()
  compact()

  env.sparkCtx.stop()
}
