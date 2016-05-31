package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag

object twolevel extends LazyLogging {
  import primitives.{FilterReaderWriter, MutationsReaderWriter, RBLogReader}
  import primitives.implicits.{read, readOrGetEmpty, write}

  implicit object FilterCompactor extends Compactible[FilterTuple] {
    override def needsCompaction(pg: PGroup[FilterTuple], home: String): Runnable[Boolean] = {
      Runnable(env => {
        val path = l0.path(home, pg, CStoreProps.FilterFileExtension(env.conf))
        Utils.listFiles(env.fs, new Path(path)) match {
          case Some(_) => true
          case None => false
        }
      })
    }

    override def compactAndPersist(pg: PGroup[FilterTuple], home: String, temp: String)
    : Runnable[Unit] = {
      Runnable(env => {
        for {
          f <- compactFilter(env, pg, home)
          d = l1.path(temp, pg, CStoreProps.FilterFileExtension(env.conf))
          r = write(f, d)
        } yield r(env)
      })
    }

    override def moveAndCleanup(pg: PGroup[FilterTuple], temp: String, home: String)
    : Runnable[Unit] = {
      Runnable(env => {
        val fExt = CStoreProps.FilterFileExtension(env.conf)
        val mExt = CStoreProps.MutationsFileExtension(env.conf)

        env.fs.rename(new Path(l1.path(temp, pg, fExt)), new Path(l1.path(home, pg, fExt)))
        env.fs.rename(new Path(l0.path(home, pg, mExt)), new Path(l1.path(home, pg, mExt)))
        env.fs.delete(new Path(l0.path(home, pg, ".*")), true)
      })
    }
  }

  implicit object MutationsCompactor extends Compactible[MutationTuple] {
    override def needsCompaction(pg: PGroup[MutationTuple], home: String): Runnable[Boolean] = {
      Runnable(env => {
        val path = l1.path(home, pg, CStoreProps.MutationsFileExtension(env.conf))
        val threshold = CStoreProps.TwoLevelPGroupL1CompactionThreshold(env.conf).toInt

        Utils.listFiles(env.fs, new Path(path))
          .filter(a => a.length > threshold)
          .map(_ => true)
          .getOrElse(false)
      })
    }

    override def compactAndPersist(pg: PGroup[MutationTuple], home: String, temp: String)
    : Runnable[Unit] = {
      Runnable(env => {
        for {
          m <- compactMutations(env, pg, home)
          d = l1.path(temp, pg, CStoreProps.MutationsFileExtension(env.conf))
          r = write(m, d)
        } yield r(env)
      })
    }

    override def moveAndCleanup(pg: PGroup[MutationTuple], temp: String, home: String)
    : Runnable[Unit] = {
      Runnable(env => {
        val mExt = CStoreProps.MutationsFileExtension(env.conf)
        env.fs.rename(new Path(l1.path(temp, pg, mExt)), new Path(l1.path(home, pg, mExt)))
      })
    }
  }

  implicit object CompactedMutationsReader extends Readable[MutationTuple] {
    override def read(pg: PGroup[MutationTuple], home: String): Runnable[RDD[MutationTuple]] = {
      Runnable(env => compactMutations(env, pg, home).get)
    }
  }

  private def compactFilter(env: Env, pg: PGroup[FilterTuple], home: String)
  : Transformable[RDD[FilterTuple]] = {
    val fExt = CStoreProps.FilterFileExtension(env.conf)
    val rExt = CStoreProps.RBLogFileExtension(env.conf)

    val f0Path = l0.path(home, pg, fExt)
    val f1Path = l1.path(home, pg, fExt)
    val r0Path = l0.path(home, pg, rExt)
    logger.info(s"f0Path: $f0Path, f1Path: $f1Path, r0Path: $r0Path")

    val f0 = read[FilterTuple](f0Path)
    val f1 = readOrGetEmpty[FilterTuple](f1Path)
    val r0 = readOrGetEmpty[RBLogTuple](r0Path)

    Pipelines.compactFilter(f1(env), f0(env), r0(env))
  }

  private def compactMutations(env: Env, pg: PGroup[MutationTuple], home: String)
  : Transformable[RDD[MutationTuple]] = {
    val fExt = CStoreProps.FilterFileExtension(env.conf)
    val mExt = CStoreProps.MutationsFileExtension(env.conf)

    val f1Path = l1.path(home, pg, fExt)
    val m1Path = l1.path(home, pg, mExt)
    logger.info(s"f1Path: $f1Path, m1Path: $m1Path")

    val f1 = read[FilterTuple](f1Path)
    val m1 = read[MutationTuple](m1Path)

    Pipelines.compactMutations(m1(env), f1(env))
  }
}

private object Pipelines {
  def compactFilter: (RDD[FilterTuple], RDD[FilterTuple], RDD[RBLogTuple]) =>
      Transformable[RDD[FilterTuple]] = {
    (f1, f0, r0) => {
      import primitives.{FilterDeduplicator, FilterKeysBroadcaster, RBLogBroadcaster}
      import primitives.implicits.{broadcast, deduplicate}

      def isValidKey: (FilterTuple, Broadcast[Set[String]]) => Boolean =
        (f, b) => !b.value(f.key)
      def isValidSeqno: (FilterTuple, Broadcast[Map[(Short, Long), Long]]) => Boolean =
        (f, b) => f.seqNo() < b.value.getOrElse((f.partitionId(), f.uuid()), f.seqNo() + 1)

      for {
        r0a <- Transformable(r0)
        r0b <- broadcast(r0a)
        f0a <- Transformable(f0) if f0a.filter(f => isValidSeqno(f, r0b))
        f0b <- deduplicate(f0a)
        f0c <- broadcast(f0b)
        f1a <- Transformable(f1) if f1a.filter(f => isValidSeqno(f, r0b) && isValidKey(f, f0c))
      } yield f1a ++ f0b
    }
  }

  def compactMutations: (RDD[MutationTuple], RDD[FilterTuple]) =>
      Transformable[RDD[MutationTuple]] = {
    (m1, f1) => {
      import primitives.FilterSeqnoTuplesBroadcaster
      import primitives.implicits.broadcast

      def isValidSeqnoTuple: (MutationTuple, Broadcast[Set[(Short, Long, Long)]]) => Boolean =
        (m, b) => b.value((m.partitionId(), m.uuid(), m.seqNo()))

      for {
        f1a <- Transformable(f1)
        f1b <- broadcast(f1a)
        m1a <- Transformable(m1) if m1a.filter(m => isValidSeqnoTuple(m, f1b))
      } yield m1a
    }
  }
}

  sealed trait level {
    def path(base: String, pg: PGroup[_], ext: String) =
      List(base, pg.bucket, this.toString, pg.id).mkString("", "/", "") + ext
  }
  case object l0 extends level
  case object l1 extends level
