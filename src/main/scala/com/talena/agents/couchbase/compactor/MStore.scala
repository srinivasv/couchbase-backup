package com.talena.agents.couchbase.compactor

import org.apache.spark.sql.DataFrame

sealed abstract class MStore
case class L0(psid: String, location: String, store: MData)
  extends MStore
case class L1(psid: String, location: String, store: Option[MData])
  extends MStore
case class L2(psid: String, location: String, store: Option[MData])
  extends MStore

case class MData(mutations: Mutation, filter: Filter,
  failoverLog: Option[FailoverLog])

sealed abstract class MDataClass
case class Mutation(dataFrame: DataFrame, files: Array[String],
  format: FileFormat) extends MDataClass
case class Filter(dataFrame: DataFrame, file: String, format: FileFormat)
  extends MDataClass
case class FailoverLog(dataFrame: DataFrame, file: String, format: FileFormat)
  extends MDataClass

sealed abstract class Schema
case class MutationSchema(pid: Int, seqno: Long, key: String, value: String,
  meta: String, data: String) extends Schema
case class FilterSchema(pid: Int, seqno: Long, key: String) extends Schema
case class FailoverLogSchema(pid: Int, seqno: Long) extends Schema

sealed abstract class FileFormat
case object CSV extends FileFormat
case object JSON extends FileFormat
case object SequenceFile extends FileFormat
