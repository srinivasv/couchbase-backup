package com.talena.agents.couchbase.mstore.strategy

import com.talena.agents.couchbase.core.{CouchbaseLongRecord => MutationTuple}
import com.talena.agents.couchbase.mstore._

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

object MutationsMappingStrategy extends LazyLogging {
  def apply[A: ClassTag](env: Env)
  : (Mutations, Filter, MutationTuple => A) => MappedMutations[A] = {
    MStoreProps.MutationsMappingStrategy(env.conf) match {
      case "SparkRDD" =>
        println(s"Using mutations mapping strategy: SparkRDD")
        logger.info(s"Using mutations mapping strategy: SparkRDD")
        usingSparkRDD[A](env) _
      case unsupported => throw new IllegalArgumentException(
        "Unsupported mutations compaction strategy: " + unsupported)
    }
  }

  /** Uses Spark RDD operations for transforming mutations.
    *
    * For correctness reasons, compaction is done only if all of the following conditions are met,
    * which are specified by means of pattern matching so that the compiler can enforce them at
    * compile time itself:
    * - Mutations being mapped are PersistedMutations. There is no use case for other mutations to
    *   be mapped.
    * - SeqnoTuples of the filter must have already been broadcasted. This check is necessary as
    *   otherwise we will end up with incorrect results.
    *
    * We use Spark's mapPartitions() API which allows us to process an entire partition at a time.
    *
    * Mapping steps:
    * - Read the broadcasted filter seqno tuples into a scala variable seqoTuples which is a Set.
    * - For each mutation tuple m check whether its seqno tuple exists in the set seqnoTuples.
    * - If it does, "yield" mappingFunc(m).
    *
    *
    * @param mutations The mutations that will be mapped
    * @param filter The filter that will be used as a reference
    * @param mappingFunc The function that will do the actual mapping
    * @param env A reference to an active Env object for involing Spark operations
    * @return The mapped mutations as a new MappedMutations object
    */
  private def usingSparkRDD[A: ClassTag](env: Env)(mutations: Mutations, filter: Filter,
      mappingFunc: MutationTuple => A): MappedMutations[A] = {
    (mutations, filter) match {
      case (PersistedMutations(rdd, _, _), BroadcastedSeqnoTuples(bcast, _)) => MappedMutations[A](
          rdd.mapPartitions[A]({ iter =>
            println(s"Mapping mutations using SparkRDD")
            logger.info(s"Mapping mutations using SparkRDD")
            val seqnoTuples = bcast.value
            for {
              m <- iter if seqnoTuples((m.partitionId(), m.uuid(), m.seqNo()))
            } yield mappingFunc(m)
          }, preservesPartitioning = true),
          env)

      case unsupported => throw new IllegalArgumentException(
        "Unsupported types: " + unsupported)
    }
  }
}
