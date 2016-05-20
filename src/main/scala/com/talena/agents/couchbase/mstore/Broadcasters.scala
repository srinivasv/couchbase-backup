package com.talena.agents.couchbase.mstore

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

object Broadcasters extends LazyLogging {
  implicit object FilterKeysBroadcaster extends Broadcastable[FilterTuple, Set[String]] {
      override def broadcast(rdd: RDD[FilterTuple]): MState[Broadcast[Set[String]]] = {
      MState(rdd.sparkContext.broadcast(rdd
        .map(t => t.key())
        .collect()
        .toSet))
    }
  }

  implicit object FilterSeqnoTuplesBroadcaster extends Broadcastable[FilterTuple, Set[(Short, Long, Long)]] {
    override def broadcast(rdd: RDD[FilterTuple]): MState[Broadcast[Set[(Short, Long, Long)]]] = {
      MState(rdd.sparkContext.broadcast(rdd
        .map(t => (t.partitionId(), t.uuid(), t.seqNo()))
        .collect()
        .toSet))
    }
  }

  implicit object RBLogBroadcaster extends Broadcastable[RBLogTuple, scala.collection.Map[(Short, Long), Long]] {
    override def broadcast(rdd: RDD[RBLogTuple]): MState[Broadcast[scala.collection.Map[(Short, Long), Long]]] = {
      MState(rdd.sparkContext.broadcast(rdd
        .map(t => ((t.partition(), t.uuid()), t.seqno()))
        .collectAsMap()))
    }
  }
}
