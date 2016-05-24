package com.talena.agents.couchbase.cstore

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf

object CStoreProps extends LazyLogging {
  case object Iterable {
    def apply(c: SparkConf) = c.get("mstore.iterable", "SequentialIterable")
  }

  case object TwoLevelPGroupL1CompactionThreshold {
    def apply(c: SparkConf) = c.get("mstore.twoLevelPGroup.l1CompactionThreshold", "10")
  }

  case object MutationsFileExtension {
    def apply(c: SparkConf) = c.get("mstore.mutations.file.extension", ".mutations")
  }

  case object MutationsFileInputFormat {
    def apply(c: SparkConf) = c.get("mstore.mutations.file.inputFormat", "SequenceFile")
  }

  case object MutationsFileOutputFormat {
    def apply(c: SparkConf) = c.get("mstore.mutations.file.outputFormat", "SequenceFile")
  }

  case object FilterFileExtension {
    def apply(c: SparkConf) = c.get("mstore.filter.file.extension", ".filter")
  }

  case object FilterFileInputFormat {
    def apply(c: SparkConf) = c.get("mstore.filter.file.inputFormat", "SequenceFile")
  }

  case object FilterFileOutputFormat {
    def apply(c: SparkConf) = c.get("mstore.filter.file.outputFormat", "SequenceFile")
  }

  case object RBLogFileExtension {
    def apply(c: SparkConf) = c.get("mstore.rblog.file.extension", ".rblog")
  }

  case object RBLogFileInputFormat {
    def apply(c: SparkConf) = c.get("mstore.rblog.file.inputFormat", "CSV")
  }
}
