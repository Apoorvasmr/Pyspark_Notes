# Chapter 1: Apache Spark Architecture and Fundamentals

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

When a Spark application starts, its **driver** creates the `SparkSession` and plans the work as jobs, stages, and tasks. The **cluster manager** allocates resources, allowing **executors** on worker nodes to process data partitions in parallel. Executors report task results and status back to the driver, which coordinates the application through completion.

```text
 User application
       |
       v
    Driver  <------>  Cluster manager
 (plans work)        (allocates resources)
       |
  schedules tasks
       v
 Executors on worker nodes
 (process data partitions)
```

### Driver, SparkSession, and SparkContext

The **driver** is the application's control center: it runs the main program, plans jobs, breaks them into stages and tasks, and tells executors what to do.

- **SparkSession** is the entry point used by most modern Spark applications. It lets your program work with DataFrames and SQL.
- **SparkContext** connects the driver to Spark's execution environment and cluster. A SparkSession typically manages or exposes it.

In short, your code describes the work through the SparkSession; the driver uses the SparkContext to coordinate that work. The driver must be able to communicate with executors throughout the application. In **client mode**, it runs in the submitting process or environment; in **cluster mode**, the cluster manager launches it inside the cluster.

### Cluster manager, master, and resource manager

The **cluster manager** decides where application processes can run and allocates CPU and memory. It is like a resource coordinator: it provides places for Spark's driver or executors to run, while Spark itself plans and executes the data-processing work.

Spark supports **Standalone**, **Hadoop YARN**, and **Kubernetes**:

- In **Spark Standalone**, the **master** coordinates the cluster and **worker** services provide resources on machines.
- With **YARN**, the **ResourceManager** and **NodeManagers** manage cluster resources.
- With **Kubernetes**, Kubernetes schedules and manages containers (pods).

The driver and executors are Spark application roles; master/worker, ResourceManager/NodeManager, and Kubernetes pods describe how different cluster managers provide resources.

### Executors, JVMs, and tasks

An **executor** is a process for one Spark application, running on a worker node. It performs assigned **tasks**—usually processing individual data partitions—and can keep that application's data in memory or on disk for reuse. More executors and partitions can allow more parallel work, subject to available resources.

Spark's core execution engine runs on the **Java Virtual Machine (JVM)**. Executors are JVM processes, and a worker machine may also run a separate cluster-manager service. Each application has its own executors, so different Spark applications do not directly share executor memory.

### Where the YARN ApplicationMaster and containers fit

**ApplicationMaster (AM)** and **container** are specifically YARN concepts, not universal Spark components:

- YARN's ResourceManager allocates resources; NodeManagers manage containers on individual machines.
- The ApplicationMaster coordinates resource requests for a YARN application.
- In YARN **cluster mode**, the Spark driver runs inside the ApplicationMaster container.
- In YARN **client mode**, the driver stays in the client process, while the ApplicationMaster requests resources for executors.
- Executors run in containers allocated on worker nodes.

So, when a diagram shows an ApplicationMaster and containers, it is describing Spark running on YARN. Standalone and Kubernetes have their own resource-management terminology.

## What happens in a PySpark application?

PySpark lets you write a Spark application in Python while Spark's core engine runs on the JVM. The **Py4J** bridge carries communication between the Python driver and Spark's JVM-side driver.

1. `spark-submit` starts your Python application (for example, `main.py`).
2. The Python driver creates a `SparkSession` and describes the computation.
3. Through Py4J, the driver communicates with Spark's JVM, which plans jobs and coordinates execution.
4. The cluster manager provides resources, and JVM executors run tasks on partitions across worker nodes.
5. If a task needs to execute Python code, such as a Python function or UDF, an executor can start a Python worker process to run it.

```text
 Python application / driver
             |
           Py4J
             v
       JVM driver (Spark)
             |
       JVM executors on workers
       (Python workers as needed)
```

Py4J does not mean all work runs in Python: many DataFrame operations execute in Spark's JVM engine, while Python workers are used for Python-defined operations that need to run on executors.


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
