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

  implicit object FilterCompactor extends Compactible[Filters, Mainline] {
    override def compact(pg: PGroup[Filters], _unused: Mainline, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0, l1) = (l0Path(pg), l1Path(pg))
        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"L0 location: $l0, L1 location: $l1")
        logger.info(s"Compacted L1 filter for $pg will be written to $dest")

        val f0 = readOrAbort[FilterTuple](l0, s"L0 filter file missing for $pg")
        val f1 = readOrGetEmpty[FilterTuple](l1)
        val r0 = readOrGetEmpty[RBLogTuple](l0)

        for {
          f1a <- Pipelines.compactFilter(f1(env), f0(env), r0(env))
          r = write(f1a, dest)
        } yield(r(env))
      })
    }

    override def move(pg: PGroup[Filters], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[Filters]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object MutationsCompactor extends Compactible[Mutations, Snapshot] {
    override def compact(pg: PGroup[Mutations], snap: Snapshot, to: String): Runnable[Unit] = {
      Runnable(env => {
        val l1 = l1PathForSnapshot(snap.id)(pg)
        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"L1 location: $l1")
        logger.info(s"Compacted L1 mutations for $pg will be written to $dest")

        val fs = FileSystem.get(env.sc.hadoopConfiguration)
        val fullPath = l1 + CStoreProps.MutationsFileExtension(env.sc.getConf)
        Utils.listFiles(fs, new Path(fullPath))

        val f1 = readOrAbort[FilterTuple](l1, s"L1 filter file missing for $pg")
        val m1 = readOrAbort[MutationTuple](l1, s"L1 mutations files missing for $pg")

        for {
          m1a <- Pipelines.compactMutations(m1(env), f1(env))
          r = write(m1a, dest)
          } yield(r(env))
      })
    }

    override def move(pg: PGroup[Mutations], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[Mutations]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object FullCompactor extends Compactible[All, Mainline] {
    override def compact(pg: PGroup[All], _unused: Mainline, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0, l1) = (l0Path(pg), l1Path(pg))
        val dest = l1PathWithPrefix(to)(pg)
        logger.info(s"L0 location: $l0, L1 location: $l1")
        logger.info(s"Compacted L1 filter and mutations for $pg will be written to $dest")

        val f0 = readOrAbort[FilterTuple](l0, s"L0 filter file missing for $pg")
        val f1 = readOrGetEmpty[FilterTuple](l1)
        val r0 = readOrGetEmpty[RBLogTuple](l0)
        val m1 = readOrGetEmpty[MutationTuple](l1)

        for {
          f1a <- Pipelines.compactFilter(f1(env), f0(env), r0(env))
          m1a <- Pipelines.compactMutations(m1(env), f1a)
          rf = write(f1a, dest)
          rm = write(m1a, dest)
        } yield(rf(env), rm(env))
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
    override def map[C: ClassTag](pg: PGroup[Mutations], snap: Snapshot, f: MutationTuple => C)
    : Runnable[Transformable[RDD[C]]] = {
      Runnable(env => {
        val l1 = l1PathForSnapshot(snap.id)(pg)
        logger.info(s"L1 location: $l1")

        val f1 = readOrAbort[FilterTuple](l1, s"L1 filter file missing for $pg")
        val m1 = readOrAbort[MutationTuple](l1, s"L1 mutations files missing for $pg")

        for {
          m1a <- Pipelines.compactMutations(m1(env), f1(env))
        } yield(m1a.map[C](f))
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
  def compactFilter: (RDD[FilterTuple], RDD[FilterTuple], RDD[RBLogTuple]) =>
      Transformable[RDD[FilterTuple]] = {
    (f1, f0, r0) => {
      def isValidKey: (FilterTuple, Broadcast[Set[String]]) => Boolean =
        (f, b) => !b.value(f.key)
      def isValidSeqno: (FilterTuple, Broadcast[Map[(Short, Long), Long]]) => Boolean =
        (f, b) => f.seqNo() < b.value.getOrElse((f.partitionId(), f.uuid()), f.seqNo() + 1)

      import primitives.{FilterDeduplicator, FilterKeysBroadcaster, RBLogBroadcaster}
      import primitives.implicits.{broadcast, deduplicate}

      for {
        r0a <- Transformable(r0)
        r0b <- broadcast(r0a)
        f0a <- Transformable(f0) if f0a.filter(f => isValidSeqno(f, r0b))
        f0b <- deduplicate(f0a)
        f0c <- broadcast(f0b)
        f1a <- Transformable(f1) if f1a.filter(f => isValidSeqno(f, r0b) && isValidKey(f, f0c))
      } yield(f1a ++ f0b)
    }
  }

  def compactMutations: (RDD[MutationTuple], RDD[FilterTuple]) =>
      Transformable[RDD[MutationTuple]] = {
    (m1, f1) => {
      import primitives.FilterSeqnoTuplesBroadcaster
      import primitives.implicits.broadcast

      for {
        f1a <- Transformable(f1)
        f1b <- broadcast(f1a)
        m1a <- Transformable(m1) if m1a.filter(m => f1b.value((m.partitionId(), m.uuid(), m.seqNo())))
      } yield(m1a)
    }
  }
}

sealed trait level
case object l0 extends level
case object l1 extends level
