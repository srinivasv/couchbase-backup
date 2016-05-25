package com.talena.agents.couchbase.cstore.pgroup

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.core.{CouchbaseRollbackRecord => RBLogTuple}
import com.talena.agents.couchbase.core.{CouchbaseShortRecord => FilterTuple}
import com.talena.agents.couchbase.cstore._

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

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
        val (l0Path, l1Path) = (l0.path(pg), l1.path(pg))
        logger.info(s"Compacting L1 filter for $pg")

        for {
          f <- compactL1Filter(env, pg, l0Path, l1Path)
          d = l1.prefixedPath(pg, to)
          r = write(f, d)
        } yield r(env)
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

        val l1Path = l1.snapshotPath(pg, snap.id)
        mutationsCompactionCheck(env, l1Path)
          .filter({ case(a, t) => a.length > t })
          .map({ case(_, t) =>
            logger.info(s"Compacting L1 mutations for $pg. Threshold is $t files")
            for {
              m <- compactL1Mutations(env, pg, l1Path)
              d = l1.prefixedPath(pg, to)
              r = write(m, d)
            } yield r(env)
          })
          .getOrElse(s"Skipping compaction for $pg")
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
        val (l0Path, l1Path) = (l0.path(pg), l1.path(pg))
        logger.info(s"Compacting L1 filter for $pg")

        for {
          f <- compactL1Filter(env, pg, l0Path, l1Path)
          d = l1.prefixedPath(pg, to)
          rf = write(f, d)

          _ = mutationsCompactionCheck(env, l1Path)
                .filter({ case(a, t) => a.length > t })
                .map({ case(_, t) =>
                  logger.info(s"Compacting L1 mutations for $pg. Threshold is $t files")
                  for {
                    m <- compactL1Mutations(env, pg, l1Path)
                    rm = write(m, d)
                  } yield rm(env)
                })
                .getOrElse(s"Skipping compaction for $pg")
        } yield rf(env)
      })
    }

    override def move(pg: PGroup[All], from: String): Runnable[Unit] = {
      Runnable(env => { println("move") })
    }

    override def cleanup(pg: PGroup[All]): Runnable[Unit] = {
      Runnable(env => { println("cleanup") })
    }
  }

  implicit object CompactedMutationsReader extends Readable[Mutations, Snapshot, MutationTuple] {
    override def read(pg: PGroup[Mutations], snap: Snapshot): Runnable[RDD[MutationTuple]] = {
      Runnable(env => {
        val l1Path = l1.snapshotPath(pg, snap.id)
        compactL1Mutations(env, pg, l1Path).get()
      })
    }
  }

  private def mutationsCompactionCheck(env: Env, path: String): Option[(Array[FileStatus], Int)] = {
    val fs = FileSystem.get(env.sc.hadoopConfiguration)
    val fullPath = path + CStoreProps.MutationsFileExtension(env.sc.getConf)
    val threshold = CStoreProps.TwoLevelPGroupL1CompactionThreshold(env.sc.getConf).toInt
    Utils.listFiles(fs, new Path(fullPath)).map(a => (a, threshold))
  }

  private def compactL1Filter(env: Env, pg: PGroup[_], l0Path: String, l1Path: String)
  : Transformable[RDD[FilterTuple]] = {
    logger.info(s"L0 location: $l0, L1 location: $l1Path")

    val f0 = readOrAbort[FilterTuple](l0Path, s"L0 filter file missing for $pg")
    val f1 = readOrGetEmpty[FilterTuple](l1Path)
    val r0 = readOrGetEmpty[RBLogTuple](l0Path)

    Pipelines.compactFilter(f1(env), f0(env), r0(env))
  }

  private def compactL1Mutations(env: Env, pg: PGroup[_], path: String)
  : Transformable[RDD[MutationTuple]] = {
    logger.info(s"L1 location: $path")

    val f1 = readOrAbort[FilterTuple](path, s"L1 filter file missing for $pg")
    val m1 = readOrAbort[MutationTuple](path, s"L1 mutations files missing for $pg")

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
    def path(pg: PGroup[_]) = {
      mkString(List(pg.dataRepo, pg.job, pg.bucket, this.toString, pg.id))
    }

    protected def mkString(l: List[String]) = l.mkString("/", "/", "")
  }

  case object l0 extends level

  case object l1 extends level {
    def prefixedPath(pg: PGroup[_], prefix: String) = {
      mkString(List(prefix, pg.dataRepo, pg.job, pg.bucket, this.toString, pg.id))
    }

    def snapshotPath(pg: PGroup[_], snap: String) = {
      mkString(List(pg.dataRepo, pg.job, snap, pg.bucket, this.toString, pg.id))
    }
  }
