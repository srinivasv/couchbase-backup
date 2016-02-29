package com.talena.agents.couchbase.mstore

import org.apache.hadoop.fs.{FileSystem, Path}

object Utils {
  def verifyLocation(fs: FileSystem, location: String): Boolean = {
    val path = new Path(location)
    if (!fs.exists(path))
      throw new IllegalArgumentException(location + " does not exist")
    if (!fs.getFileStatus(path).isDirectory)
      throw new IllegalArgumentException(location + " is not a directory")
    true
  }
}
