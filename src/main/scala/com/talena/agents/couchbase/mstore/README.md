# MStore
MStore (Mutations Store) is a mutations storage and management API for Couchbase and will be the single point of access for all data manipulation operations such as restores (both full and incremental), masking and sampling.

## Motivation
Couchbase does not support snapshots at the file system layer like Cassandra does. Hence, the Couchbase guys have recommended that we use their Database Change Protocol (DCP) mechanism for incremental backups. DCP is their standard data streaming protocol which they use for their internal replication, their cross data center active-active replication as well in their own backup utility called cbbackup.

KV-pairs data in Couchbase is organized into user defined buckets. Internally, for load balancing reasons, Couchbase also partitions a bucket into (usually) 1024 vbuckets (virtual buckets), and hashes the incoming keys to one of them. Within **each** vbucket, there is a global monotonically increasing 64-bit sequence number that is incremented whenever a key is mutated. DCP tracks this sequence number for each vbucket and has APIs that, given a range of sequence numbers, can stream all mutations that fall within the range.

So at a high level, incremental backup will work as follows: In our catalog, we track the last seen sequence numbers for all the vbuckets, whose buckets we're managing for backup/mirroring purposes. In each backup run, we query the Couchbase server for the "current high" sequence numbers and issue DCP streaming requests for the range (last_seen_seqno, current_high_seqno].

While this works well, it leads to a storage problem because over subsequent backup runs, we will accumulate a lot of stale data. Couchbase's own backup utility cbbackup has this problem because of which they recommend the traditional periodic-full-plus-incremental approach to their customers. The restores, too, take longer because one needs to start with the last full backup followed by replaying all the incrementals since then.

Both the storage and long restore time problems can be solved by periodically compacting the stored mutations. MStore is an extensible mutations storage and management API to expose a single unified interface for multiple use cases. The MStore API will be used by the scheduler, recovery, masking and sampling agents to periodically prune stale mutations as well as emit the live ones on demand.

## Overview
As the data mover receives mutations from the Couchbase server, it writes two sets of files for each vbucket:
* A data file containing the DCP mutations in their entirety (the key, the value and some miscellaneous metadata). This is called the *mutations* file.
* A secondary file containing just the key and the sequence number at which it was mutated. This is called the *filter* file.

For **each** vbucket, storage management happens in the following phases:
* After the data mover finishes capturing all available mutations, the filter from the current run is first deduplicated to remove all but the latest occurrence of each key. This is called *filter deduplication*.
* The mainline version of the filter (which corresponds to its state as of the previous run) is then updated by propagating changes from the deduplicated filter from above. This is called *filter compaction* because we're eliminating keys that were live as of the last run but are now stale because of updates since then.
* The mainline version of the mutations is then updated using an updated filter (one where all the stale keys have been eliminated). This is called *mutations compaction*.

Filters carry a very small amount of data (just a key and a sequence number), so keeping them up to date during each backup run should be acceptable from an execution time perspective. Also, because the filters are always up to date, it gives us the freedom to schedule mutations compaction (which will be an expensive operation) either less frequently and/or offline. If a restore were to be required before the mutations have been compacted, we can use the filters to filter out the stale mutations before restoring them at the destination.

The filter deduplication, filter compaction and mutations compaction operations have pipelined nature to them because they need to happen in a certain order. Also, these operations need to be repeated for all of the 1024 vbuckets of a Couchbase bucket, and there could be multiple such buckets in a single workflow. For these reasons, Spark is a better fit than MapReduce for this problem.

Consequently, MStore internally uses Spark for all of its data manipulation operations, and is written in Scala.

## Offline Mutations Compaction
TBA

## MStore Architecture

#### Terminology
* A partition in MStore corresponds to a specific vbucket in Couchbase.
* A partition group similarly corresponds to a group of vbuckets -- grouped for performance reasons -- with the group size being configurable.

#### Partition Group
A PartitionGroup is the smallest unit of computation in MStore and comprises a set of Couchbase bucket partitions that are always backed up, compacted, mapped and restored as a single unit.

PartitionGroup is modeled using what are called Algebraic Data Types (ADTs) in functional programming. The idea is to define a rigorous type system that mimics the business functions being modeled (compaction, recovery, masking/sampling operations in our case) and then let the compiler do static type checks on our code. This, along with pattern matching, will ensure that buggy code doesn't even get compiled, thus eliminating large classes of bugs altogether.

#### Components
* **MStore API**: This is the entry point for all the external entities, such as the scheduler and the restore, masking and sampling agents.
* **Managers**: A manager is responsible for a specific layout of the filters and mutations files and for carrying out compaction and filtering operations of a **single partition group**. Currently, an implementation for a 2-level manager (L0/L1) is available. If required, we can add a 3-level manager (L0/L1/L2) in the future.
* **Iterables**: An iterable specifies a traversal order over all possible partition groups given a set of Couchbase buckets. An iterable decouples the traversal of partition groups from the operations that must be applied on them and gives us flexibility into evolving more complex forms of traversals without changing the rest of the code. Currently, a simple sequential iterable has been implemented, which enumerates all the partition groups of all the buckets and visits them one by one in sequential order. We will add the following iterables in the future:
 * A parallel iterable that can visit multiple Couchbase buckets in parallel. This can be used when the number of Couchbase buckets is large.
 * A parallel iterable that can visit multiple partition groups within a Couchbase bucket in parallel. This can be used when the number of Couchbase buckets is small so that we still exploit parallelism within each bucket.
* **Strategies**: A strategy determines how a specific operation such as filter deduplication, filter compaction or mutations filtering should be executed. A separate strategy module decouples both the management and traversal aspects of partition groups from **how** operations must be executed. This also makes it very trivial to add new optimized strategies without disturbing any of the existing code.

#### MStore APIs

* ```compact(dataRepo: String, job: String, bProps: List[BucketProps], dest: String)```

  Compacts both the filters and mutations off a *snapshot*, compacting the mutations after the filters have been compacted.

* ```compactFilters(dataRepo: String, job: String, bProps: List[BucketProps], dest: String)```

  Compacts all the filters for a given job off the *mainline*. This method may be invoked on its own allowing us to decouple the filter and mutations compaction operations so that mutations compaction may be run as an offline process if desired.

* ```compactMutations(dataRepo: String, job: String, bProps: List[BucketProps], snapshot: String, dest: String)```

  Compacts all the mutations for a give job off a *snapshot* if the compaction criteria is met. This method may be invoked in an offline mode so it could run as a background process.

* ```mapMutations[A](dataRepo: String, job: String, bProps: List[BucketProps], snapshot: String, callable: MutationsMapper[A])```

  "Maps" all mutations from a give snapshot using the given function (a callback). This allows us to arbitrarily transform a set of mutations any number of times in a composable manner. The caller will have to extend the MutationsMapper[A] class and override the map() method. This map() method will be called by mapMutations() as it reads the mutations and filters them based on whether they're live or stale. Check out *RestoreExample.java* in the repository for an example of how restores could be done using this API. We will use the same API to implement masking/sampling as well in the future.

#### Source Code Files
The organization of the most important files/directories is as follows. There is a single Configuration object that gets passed around to all the modules. Each component picks from it the properties that are of interest to it. This gives us a lot of flexibility in choosing how we want to execute MStore operations.

```
mstore/MStore.scala => Contains all the external facing APIs described above.
mstore/PartitionGroup.scala => Describes the PartitionGroup ADT.

mstore/iterable/MIterable.scala => The interface for implementing new iterables.
mstore/iterable/SequentialIterable.scala => A simple sequential iterator over all partition groups of a given set of Couchbase buckets.

mstore/manager/PartitionGroupManager.scala => The interface for implementing new partition group managers.
mstore/manager/TwoLevelPartitionGroupManager.scala => A 2 level (L0/L1) manager.

mstore/strategy/FilterCompactionStrategy.scala => All strategies that could be used to compact higher level (L1/L2) filters.
mstore/strategy/FilterDeduplicationStrategy.scala => All strategies that could be used to deduplicate the lowest level (L0) filter.
mstore/strategy/MutationsMappingStrategy.scala => All strategies that could be used to "map" mutations using arbitrary transform functions. Both the compaction and recovery code paths lead to this strategy. In the future, masking/sampling agents will also use this same code path.
```

## Scala Notes
#### Option Class
Option is used whenever applicable. This gives strong type safety and checking and eliminates all NPEs for good. Option also supports map(), filter() and other useful methods to make error handling easy and intuitive.
* http://www.tutorialspoint.com/scala/scala_options.htm
* http://www.scala-lang.org/api/current/#scala.Option

#### Apply() method
The Scala apply() method is used in a number of places. This allows for some nice syntactic sugar.
* https://twitter.github.io/scala_school/basics2.html#apply

#### Sealed Traits/AbstractClasses and Case Classes
Sealed traits and abstract classes, and case classes are used to model Algebraic Data Types (ADTs). Marking something as "sealed" allows the compiler to do an exhaustive type check of all alternatives of a parent trait/class during pattern matching. Thus, if we do not handle an alternative in the case clause, the compiler will warn us.
* http://docs.scala-lang.org/tutorials/tour/case-classes.html

#### Companion Objects
Companion objects are extensively used. Companion objects share the same name as the class for which they are a companion and both have complete access to each other's members and methods. Companion objects typically hold all of a class' static methods including any factory methods
to create instances of the class or any of its subclasses.
* http://docs.scala-lang.org/tutorials/tour/singleton-objects.html

#### For Comprehensions
Scala's for-comprehensions are used to iterate over filter and mutations tuples of a Spark partition during deduplication, compaction and mapping operations.
* http://alvinalexander.com/scala/scala-for-loop-yield-examples-yield-tutorial
