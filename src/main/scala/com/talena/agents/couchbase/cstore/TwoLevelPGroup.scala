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

  implicit object FilterCompactor extends Compactible[Filters] {
    override def compact(pg: PGroup[Filters], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0Path, l1Path) = (l0.path(from, pg), l1.path(from, pg))
        logger.info(s"Compacting L1 filter for $pg")

        for {
          f <- compactFilter(env, l0Path, l1Path)
          d = l1.path(to, pg)
          r = write(f, d)
        } yield r(env)
      })
    }

    override def move(pg: PGroup[Filters], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        moveCompacted(env, pg, from, to, CStoreProps.FilterFileExtension(env.conf))
        env.fs.rename(new Path(l0.path(to, pg) + CStoreProps.MutationsFileExtension(env.conf)),
          new Path(l1.path(to, pg) + CStoreProps.MutationsFileExtension(env.conf)))
      })
    }
  }

  implicit object MutationsCompactor extends Compactible[Mutations] {
    override def compact(pg: PGroup[Mutations], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        val l1Path = l1.path(from, pg)
        mutationsCompactionCheck(env, l1Path)
          .filter({ case(a, t) => a.length > t })
          .map({ case(_, t) =>
            logger.info(s"Compacting L1 mutations for $pg. Threshold is $t files")
            for {
              m <- compactMutations(env, l1Path)
              d = l1.path(to, pg)
              r = write(m, d)
            } yield r(env)
          })
          .getOrElse(logger.info(s"Skipping compaction for $pg"))
      })
    }

    override def move(pg: PGroup[Mutations], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        moveCompacted(env, pg, from, to, CStoreProps.MutationsFileExtension(env.conf))
      })
    }
  }

  implicit object FullCompactor extends Compactible[All] {
    override def compact(pg: PGroup[All], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        val (l0Path, l1Path) = (l0.path(from, pg), l1.path(from, pg))
        logger.info(s"Compacting L1 filter for $pg")

        val res = for {
          f <- compactFilter(env, l0Path, l1Path)
          d = l1.path(from, pg)
          rf = write(f, d)

          _ = mutationsCompactionCheck(env, l1Path)
                .filter({ case(a, t) => a.length > t })
                .map({ case(_, t) =>
                  logger.info(s"Compacting L1 mutations for $pg. Threshold is $t files")
                  for {
                    m <- compactMutations(env, l1Path)
                    rm = write(m, d)
                  } yield rm(env)
                })
                .getOrElse(logger.info(s"Skipping compaction for $pg"))
        } yield rf(env)
      })
    }

    override def move(pg: PGroup[All], from: String, to: String): Runnable[Unit] = {
      Runnable(env => {
        moveCompacted(env, pg, from, to, CStoreProps.MutationsFileExtension(env.conf))
        moveCompacted(env, pg, from, to, CStoreProps.FilterFileExtension(env.conf))
      })
    }
  }

  implicit object CompactedMutationsReader extends Readable[Mutations, MutationTuple] {
    override def read(pg: PGroup[Mutations], from: String): Runnable[RDD[MutationTuple]] = {
      Runnable(env => {
        val l1Path = l1.path(from, pg)
        compactMutations(env, l1Path).get
      })
    }
  }

  private def compactFilter(env: Env, l0Path: String, l1Path: String)
  : Transformable[RDD[FilterTuple]] = {
    logger.info(s"L0 location: $l0Path, L1 location: $l1Path")

    val f0 = readOrAbort[FilterTuple](l0Path, s"L0 filter file missing: $l0Path")
    val f1 = readOrGetEmpty[FilterTuple](l1Path)
    val r0 = readOrGetEmpty[RBLogTuple](l0Path)

    Pipelines.compactFilter(f1(env), f0(env), r0(env))
  }

  private def compactMutations(env: Env, path: String): Transformable[RDD[MutationTuple]] = {
    logger.info(s"L1 location: $path")

    val f1 = readOrAbort[FilterTuple](path, s"L1 filter file missing: $path")
    val m1 = readOrAbort[MutationTuple](path, s"L1 mutations files missing: $path")

    Pipelines.compactMutations(m1(env), f1(env))
  }

  private def moveCompacted(env: Env, pg: PGroup[_], from: String, to: String, ext: String)
  : Unit = {
    val fullFrom = l1.path(from, pg) + ext
    val fullTo = l1.path(to, pg) + ext
    logger.info(s"Moving compacted file from $fullFrom to $fullTo")

    env.fs.rename(new Path(fullFrom), new Path(fullTo))
  }

  private def mutationsCompactionCheck(env: Env, path: String): Option[(Array[FileStatus], Int)] = {
    val fullPath = path + CStoreProps.MutationsFileExtension(env.conf)
    val threshold = CStoreProps.TwoLevelPGroupL1CompactionThreshold(env.conf).toInt
    Utils.listFiles(env.fs, new Path(fullPath)).map(a => (a, threshold))
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
    def path(base: String, pg: PGroup[_]) =
      List(base, pg.bucket, this.toString, pg.id).mkString("", "/", "")
  }
  case object l0 extends level
  case object l1 extends level
