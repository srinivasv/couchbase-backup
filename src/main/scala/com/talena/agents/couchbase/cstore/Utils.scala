package com.talena.agents.couchbase.cstore

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf

object Utils extends LazyLogging {
  def isValidLocation(fs: FileSystem, location: Path): Boolean = {
    fs.exists(location) && fs.getFileStatus(location).isDirectory
  }

  def listFiles(fs: FileSystem, glob: Path): Option[Array[FileStatus]] = {
    val files = fs.globStatus(glob)
    if (files == null) None else Some(files).filter(f => f.length > 0)
  }
}
