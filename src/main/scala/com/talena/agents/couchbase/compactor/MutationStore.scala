package com.talena.agents.couchbase.compactor

import org.apache.spark.sql.DataFrame

abstract class MutationStore
case class L0(store: DataFramePair, props: MutationStoreProps)
  extends MutationStore
case class L1(store: Option[DataFramePair], props: MutationStoreProps)
  extends MutationStore
case class L2(store: Option[DataFramePair], props: MutationStoreProps)
  extends MutationStore

case class DataFramePair(mutations: DataFrame, filter: DataFrame)

case class MutationStoreProps(psId: String, level: MutationStoreLevel,
  location: String, format: FileFormat)

abstract class MutationStoreLevel
case object L0 extends MutationStoreLevel
case object L1 extends MutationStoreLevel
case object L2 extends MutationStoreLevel

abstract class FileFormat
case object CSV extends FileFormat
case object SequenceFile extends FileFormat
