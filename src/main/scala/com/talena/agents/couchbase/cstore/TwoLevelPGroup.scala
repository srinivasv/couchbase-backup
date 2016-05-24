package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag

object twolevel extends LazyLogging {
  import primitives.{FilterReaderWriter, MutationsReaderWriter, RBLogReader}
  import primitives.implicits.{readOrAbort, readOrGetEmpty, write}

  implicit object FilterCompactor extends Compactible[Filters] {
    override def compact(pg: PGroup[Filters], src: Source, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0, l1) = (l0Path(pg), l1Path(pg))
        logger.info(s"L0 location: $l0, L1 location: $l1")

        val f0 = readOrAbort[FilterTuple](l0, s"L0 filter file missing for $pg")
        val f1 = readOrGetEmpty[FilterTuple](l1)
        val r0 = readOrGetEmpty[RBLogTuple](l0)

        val f1Compacted = Pipelines.compactFilter(f1(env), f0(env), r0(env))

        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"Persisting compacted L1 filter for $pg to path $dest")
        write(f1Compacted.get(), dest)
      })
    }

    override def move(pg: PGroup[Filters], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[Filters]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object MutationsCompactor extends Compactible[Mutations] {
    override def compact(pg: PGroup[Mutations], src: Source, to: String): Runnable[Unit] = {
      Runnable(env => {
        val snap = src.getSnapshotOrAbort(s"Compaction source must be a snapshot.")
        val l1 = l1PathForSnapshot(snap)(pg)
        logger.info(s"L1 location: $l1")

        val f1 = readOrAbort[FilterTuple](l1, s"L1 filter file missing for $pg")
        val m1 = readOrAbort[MutationTuple](l1, s"L1 mutations files missing for $pg")

        val m1Compacted = Pipelines.compactMutations(m1(env), f1(env))

        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"Persisting compacted L1 mutations for $pg to path $dest")
        write(m1Compacted.get(), dest)
      })
    }

    override def move(pg: PGroup[Mutations], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[Mutations]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object FullCompactor extends Compactible[All] {
    override def compact(pg: PGroup[All], src: Source, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0, l1) = (l0Path(pg), l1Path(pg))
        logger.info(s"L0 location: $l0, L1 location: $l1")

        val f0 = readOrAbort[FilterTuple](l0, s"L0 filter file missing for $pg")
        val f1 = readOrGetEmpty[FilterTuple](l1)
        val r0 = readOrGetEmpty[RBLogTuple](l0)
        val m1 = readOrGetEmpty[MutationTuple](l1)

        val f1Compacted = Pipelines.compactFilter(f1(env), f0(env), r0(env))
        val m1Compacted = Pipelines.compactMutations(m1(env), f1Compacted)

        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"Persisting compacted L1 filter for $pg to path $dest")
        write(f1Compacted.get(), dest)
        logger.info(s"Persisting compacted L1 mutations for $pg to path $dest")
        write(m1Compacted.get(), dest)
      })
    }

    override def move(pg: PGroup[All], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[All]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object MutationsMapper extends Mappable[Mutations, Snapshot] {
    override def map[C: ClassTag](pg: PGroup[Mutations], src: Snapshot, f: MutationTuple => C)
    : Runnable[RDD[C]] = {
      Runnable(env => {
        val l1 = l1PathForSnapshot(src.s)(pg)
        logger.info(s"L1 location: $l1")

        val f1 = readOrAbort[FilterTuple](l1, s"L1 filter file missing for $pg")
        val m1 = readOrAbort[MutationTuple](l1, s"L1 mutations files missing for $pg")

        val m1a = Pipelines.compactMutations(m1(env), f1(env))
        m1a.get().map[C](f)
      })
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

private object Pipelines {
  def compactFilter: (Transformable[RDD[FilterTuple]], Transformable[RDD[FilterTuple]],
      Transformable[RDD[RBLogTuple]]) => Transformable[RDD[FilterTuple]] = {
    (f1, f0, r0) => {
      def isValidKey: (FilterTuple, Broadcast[Set[String]]) => Boolean =
        (f, b) => !b.value(f.key)
      def isValidSeqno: (FilterTuple, Broadcast[Map[(Short, Long), Long]]) => Boolean =
        (f, b) => f.seqNo() < b.value.getOrElse((f.partitionId(), f.uuid()), f.seqNo() + 1)

      import primitives.{FilterDeduplicator, FilterKeysBroadcaster, RBLogBroadcaster}
      import primitives.implicits.{broadcast, deduplicate}

      for {
        r0a <- r0
        r0b <- broadcast(r0a)
        f0a <- f0 if f0a.filter(f => isValidSeqno(f, r0b))
        f0b <- deduplicate(f0a)
        f0c <- broadcast(f0b)
        f1a <- f1 if f1a.filter(f => isValidSeqno(f, r0b) && isValidKey(f, f0c))
      } yield(f1a ++ f0b)
    }
  }

  def compactMutations: (Transformable[RDD[MutationTuple]], Transformable[RDD[FilterTuple]]) =>
      Transformable[RDD[MutationTuple]] = {
    (m1, f1) => {
      import primitives.FilterSeqnoTuplesBroadcaster
      import primitives.implicits.broadcast

      for {
        f1a <- f1
        f1b <- broadcast(f1a)
        m1a <- m1 if m1a.filter(m => f1b.value((m.partitionId(), m.uuid(), m.seqNo())))
      } yield(m1a)
    }
  }
}

sealed trait level
case object l0 extends level
case object l1 extends level
