# Apache Spark: Architecture and Fundamentals

These notes explain the Spark concepts listed alongside the video [Apache Spark - The Ultimate Guide (From ZERO To PRO)](https://www.youtube.com/watch?v=FNJze2Ea780). They are a companion study guide, not a word-for-word transcript. The architecture details are checked against the [official Spark cluster overview](https://spark.apache.org/docs/latest/cluster-overview.html).

## Why Apache Spark?

Apache Spark is an engine for processing large datasets in parallel. Instead of doing all the work on one machine, Spark can divide data and computation across multiple machines in a cluster. It provides APIs for Python (PySpark), Scala, Java, and R, and supports batch processing, SQL, streaming, machine learning, and graph workloads.

Spark is useful when a dataset or workload is too large or slow to handle efficiently on one machine, or when several processing steps need to be composed and run in parallel. Spark is not automatically faster for every workload: startup, coordination, shuffles, and data size all matter.

## Monolithic and distributed processing

| Monolithic (single-machine) application | Distributed application |
| --- | --- |
| One process or machine does the work. | Work is divided among processes on multiple machines. |
| Data and computation are local to that machine. | Data is split into partitions; tasks process partitions in parallel. |
| Easier to run and debug, but limited by one machine's resources. | Can scale out, but needs coordination and must handle network and machine failures. |

“Monolithic” describes how an application is organized or deployed; “distributed” describes where and how its work runs. A single-machine Spark run (`local` mode) is possible, but does not provide cluster-scale parallelism.

## Spark compared with MapReduce

Both Spark and Hadoop MapReduce can process data in parallel across a cluster. The main difference is the execution model:

| Topic | Spark | Traditional Hadoop MapReduce |
| --- | --- | --- |
| Computation | Builds a graph of transformations and executes it as jobs and stages. | Runs map and reduce phases, with data passed between phases. |
| Intermediate data | Can keep reused data in memory when explicitly persisted; otherwise it may use memory and disk as needed. | Commonly materializes intermediate map output to disk before reduce. |
| Multi-step workloads | Can avoid repeatedly rereading and rewriting data, especially when intermediate results are cached. | Chained steps commonly write intermediate results to storage between jobs. |
| APIs | Includes DataFrame/SQL and other libraries as well as lower-level APIs. | Primarily the MapReduce programming model. |

Spark does not eliminate disk I/O or replace every Hadoop component. It can read from and write to Hadoop-compatible storage, and it may spill data to disk when memory is insufficient. The advantage is often most noticeable in iterative or multi-stage workloads where intermediate data can be reused.

## Core Spark concepts

### In-memory computation

Spark can cache or persist data in executor memory so later operations can reuse it without recomputing or rereading it. This can speed up iterative algorithms and repeated queries.

“In-memory” does **not** mean that Spark loads every dataset entirely into RAM. Data can be read incrementally, cached only when requested, evicted, or spilled to disk. Use `cache()` or `persist()` when reuse makes storage worthwhile.

### Lazy evaluation

Transformations such as `select`, `filter`, and `map` describe work; Spark generally does not execute them immediately. It records the operations as a plan or lineage graph. An action such as `count`, `collect`, or `write` triggers execution.

This lets Spark optimize and combine work before running it. For example:

```python
result = (
    df.filter(df["amount"] > 0)  # Transformation: defines work
     .select("customer_id", "amount")
)

result.write.parquet("output/")  # Action: triggers execution
```

### Fault tolerance

Spark tracks how derived data was computed (its lineage). If a partition is lost, Spark can often recompute that partition from its source and transformations. Persisted data may also be replicated or recovered depending on the storage level and configuration.

Fault tolerance is not a promise that every failure is invisible: a failed source, unavailable external system, non-replayable input, or lost output can require application-level recovery. Streaming queries use checkpointing and source/sink guarantees to recover progress.

### Partitioning and parallelism

A partition is a logical chunk of a dataset. Spark schedules work on partitions; a task typically processes one partition at a time. Partitions allow a large dataset to be processed in parallel across executor cores.

Too few partitions can leave cluster resources idle; too many can add scheduling overhead and create many small tasks. Operations such as joins and `groupBy` may cause a **shuffle**: data is redistributed across executors, often over the network. Shuffles are relatively expensive, so partitioning and data-skew awareness matter.

### Batch and stream processing

- **Batch processing** works on a bounded input, such as a file or a fixed set of records, and produces a result for that batch.
- **Stream processing** handles an unbounded input as new records arrive, maintaining and updating results over time.
- **Structured Streaming** uses the DataFrame/Spark SQL model for both. By default, it processes incoming data as a sequence of micro-batches. Spark also documents a continuous processing mode with different latency and delivery guarantees.

Streaming is therefore not simply “one huge batch.” It is a long-running query that incrementally processes new data and may need event-time handling, state, checkpoints, and a durable sink.

## Cluster architecture

At a high level, a Spark application has a **driver** and one or more **executors**:

```text
User application / spark-submit
             |
             v
   Driver process
   - runs application logic
   - creates SparkSession / SparkContext
   - plans jobs and schedules tasks
             |
        Cluster manager
   (allocates resources for the app)
       /          |          \
      v           v           v
 Executor      Executor    Executor
 on worker     on worker   on worker
   tasks         tasks       tasks
 partitions    partitions  partitions
```

### Driver, SparkSession, and SparkContext

- The **driver program** runs the application's main logic, turns actions into jobs, divides jobs into stages and tasks, and coordinates execution.
- **SparkSession** is the main entry point for modern Spark SQL/DataFrame applications. It provides access to DataFrames, SQL, and the underlying Spark context.
- **SparkContext** is the lower-level connection to Spark's execution environment and cluster. A SparkSession typically manages or exposes it.

The driver must be able to communicate with executors over the network for the application's lifetime. In **client deploy mode**, the driver runs in the submitting process/environment. In **cluster deploy mode**, the cluster manager launches the driver inside the cluster.

### Cluster manager, master, and resource manager

The **cluster manager** allocates resources for Spark applications. Spark supports its **Standalone** manager, **Hadoop YARN**, and **Kubernetes**.

“Master/worker” is a useful diagram for **Spark Standalone**: the Standalone master coordinates workers, and workers provide resources to run application executors. Do not assume that every deployment has a Spark master daemon:

- On **YARN**, YARN's ResourceManager and NodeManagers manage cluster resources.
- On **Kubernetes**, Kubernetes schedules pods and manages cluster resources.
- In each case, Spark still has a driver and application executors.

The cluster manager allocates resources; it does not normally execute the user's Spark transformations itself.

### Executors, JVMs, and tasks

An **executor** is an application-specific process on a worker node. It runs tasks and can keep data in memory or on disk for that application. Each Spark application gets its own executors; different applications do not directly share executor memory.

Spark's execution engine is JVM-based. In a typical JVM-language application, the driver and executors run in JVM processes. A worker node may also run a cluster-manager service (for example, a Standalone Worker or YARN NodeManager); this service is distinct from the executor JVM launched for a particular application.

### Where the YARN ApplicationMaster and containers fit

**ApplicationMaster (AM)** and **container** are YARN terms, not names for universal Spark components:

- YARN's ResourceManager allocates resources, while NodeManagers manage containers on individual nodes.
- An application gets an ApplicationMaster to coordinate its YARN application resources.
- In YARN **cluster mode**, the Spark driver runs inside the ApplicationMaster container.
- In YARN **client mode**, the driver stays in the client process; the ApplicationMaster requests resources for the executors.
- Executors run in YARN containers allocated on worker nodes.

For Standalone or Kubernetes, use that manager's own resource and deployment terminology instead of describing it as a YARN ApplicationMaster/container setup.

## What happens in a PySpark application?

PySpark lets Python code use Spark's APIs. In the standard PySpark setup:

1. You start a Python application (for example, `main.py`) with `spark-submit`.
2. The Python driver process creates a `SparkSession`.
3. PySpark uses **Py4J** as a bridge between the Python process and Spark's JVM-side driver. The JVM hosts Spark's core scheduling and execution components.
4. The driver requests resources from the cluster manager, plans work, and schedules tasks on executors.
5. JVM executors run Spark's core tasks. When Python code must run on executors (for example, Python RDD functions or Python UDFs), Spark can launch Python worker processes alongside the executor JVMs.

```text
Python main.py (driver)  <--- Py4J --->  JVM driver (Spark)
                                      |
                               cluster manager
                                      |
               +----------------------+----------------------+
               v                                             v
        Executor JVM                                  Executor JVM
        + Python worker(s), if needed                 + Python worker(s), if needed
```

Py4J does not mean that all Spark work executes in Python. Many DataFrame operations use Spark's JVM execution engine; Python worker processes are needed for Python-defined computation that must run on the executors. The exact process layout depends on deployment mode and cluster manager.

## Terms at a glance

| Term | Meaning |
| --- | --- |
| Application | A Spark program: a driver plus its application-specific executors. |
| Driver | Coordinates a Spark application and schedules its work. |
| Cluster manager | Allocates resources across applications (Standalone, YARN, or Kubernetes). |
| Worker node | A machine that can host application code and executors. |
| Executor | An application-specific process on a worker that runs tasks and stores data. |
| Job | Parallel computation started by an action. |
| Stage | A set of tasks separated from other stages by dependency boundaries, often including shuffle boundaries. |
| Task | A unit of work sent to an executor, usually for one partition. |
| Partition | A portion of a distributed dataset that can be processed as a unit of parallel work. |

## References

- Video: [Apache Spark - The Ultimate Guide (From ZERO To PRO)](https://www.youtube.com/watch?v=FNJze2Ea780)
- Official docs: [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- Official docs: [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- Official docs: [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
- Official docs: [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
