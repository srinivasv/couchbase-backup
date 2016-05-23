package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.cstore._
//import com.talena.agents.couchbase.cstore.pgroup.Filters

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map

object twolevel extends LazyLogging {
  implicit object FilterCompactor extends Compactible[Filters] {
    override def compact(pg: PGroup[Filters], src: Source, to: String): Runnable[Unit] = {
      import primitives.{FilterDeduplicator, FilterKeysBroadcaster, FilterReaderWriter}
      import primitives.{RBLogBroadcaster, RBLogReader}
      import primitives.implicits.{broadcast, deduplicate, read, write}

      Runnable(env => {
        val conf = env.sc.getConf

        // Compute the L0 and L1 paths for this partition group
        val l0 = l0Path(pg)
        val l1 = l1Path(pg)
        logger.info(s"L0 location: $l0, L1 location: $l1")

        // Open the L0 filter for this partition group. Abort if the open fails.
        val res0 = read[FilterTuple](l0)
        val l0Filter = res0(env)
          .getOrElse(throw new IllegalStateException("L0 filter file missing: " + pg))

        // Open the L1 filter for this partition group.
        val res1 = read[FilterTuple](l1)
        val l1Filter = res1(env).getOrElse(Transformable(env.sc.emptyRDD[FilterTuple]))

        // Open the rblog for this partition group.
        val res2 = read[RBLogTuple](l0)
        val rblogOpt = res2(env)

        def isValidKey: (FilterTuple, Broadcast[Set[String]]) => Boolean =
          (f, b) => !b.value(f.key)
        def isValidSeqno: (FilterTuple, Broadcast[Map[(Short, Long), Long]]) => Boolean =
          (f, b) => f.seqNo() < b.value.getOrElse((f.partitionId(), f.uuid()), f.seqNo() + 1)

        val l1FilterCompacted = rblogOpt match {
          case Some(rblog) => for {
            r <- rblog
            rr <- broadcast(r)
            l0a <- l0Filter if l0a.filter(f => isValidSeqno(f, rr))
            l0b <- deduplicate(l0a)
            l0c <- broadcast(l0b)
            l1a <- l1Filter if l1a.filter(f => isValidSeqno(f, rr) && isValidKey(f, l0c))
          } yield(l1a)

          case None => for {
            l0a <- l0Filter
            l0b <- deduplicate(l0a)
            l0c <- broadcast(l0b)
            l1a <- l1Filter if l1a.filter(f => isValidKey(f, l0c))
          } yield(l1a)
        }

        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"Persisting compacted L1 filter for $pg to path $dest")
        write(l1FilterCompacted.get(), dest)
      })
    }

    override def move(pg: PGroup[Filters], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[Filters]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  private def l0Path = path(l0)(None, None) _
  private def l1Path = path(l1)(None, None) _
  private def l1PathWithPrefix(prefix: String) = path(l1)(Some(prefix), None) _
  private def l1PathForSnapshot(snapshot: String) = path(l1)(None, Some(snapshot)) _

  private def path(level: level)(prefix: Option[String], snapshot: Option[String])(pg: PGroup[_])
  : String = {
    Utils.buildFSPath(List(
      prefix, Some(pg.dataRepo), Some(pg.job), snapshot, Some(pg.bucket), Some(level.toString)
    )) + pg.id
  }
}

sealed trait level
case object l0 extends level
case object l1 extends level
