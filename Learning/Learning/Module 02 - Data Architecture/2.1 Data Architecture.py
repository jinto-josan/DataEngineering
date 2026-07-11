# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Data Architecture
# MAGIC
# MAGIC In this lesson you will learn more about data processing and how popular tools such as **[Apache Spark™](https://www.google.com/search?client=firefox-b-d&q=Apache+Spark)** and **[Apache Hadoop](https://hadoop.apache.org/)** processes data. In addition, you will understand what orchestration is, and some popular tools and implementations. Finally, you'll gain more knowledge in the area of  reference architecture and the CAP Theorem that will help you to get more knowledge in Data Architecture.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Know the differences of how Apache Spark and Hadoop MapReduce process data
# MAGIC * Know about orchestration, [Directed Acyclic Graph](https://sparkbyexamples.com/spark/what-is-dag-in-spark/) (DAGs) and some tools that will allow you to achieve it
# MAGIC * Be familiar with the CAP Theorem, distributed systems and trade-offs that you'll have to make in data architectures
# MAGIC * Understand and know some reference architectures from cloud vendors and others
# MAGIC
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 45-75 minutes
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Processing
# MAGIC
# MAGIC <!--<iframe width="560" height="315" src="https://www.youtube.com/embed/Uc-Wtem-lyw?si=BRy4xxxjceJ0pSZX" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>-->
# MAGIC
# MAGIC 1. Watch the following [**video**](https://www.youtube.com/embed/Uc-Wtem-lyw?si=BRy4xxxjceJ0pSZX) to understand different ways of processing data and orchestrating workflows
# MAGIC
# MAGIC 1. Read the article [**Spark vs Hadoop MapReduce**](https://www.integrate.io/blog/apache-spark-vs-hadoop-mapreduce/)
# MAGIC
# MAGIC 1. Read the article [**What is Stream Processing?**](https://hazelcast.com/glossary/stream-processing/)
# MAGIC
# MAGIC 1. Optional: Read the article [**Metadata Management: Hive Metastore vs AWS Glue**](https://lakefs.io/blog/metadata-management-hive-metastore-vs-aws-glue/)
# MAGIC
# MAGIC
# MAGIC A typical map-reduce processing pipeline reads data from the **hard drive**, performs calculations on the data and writes it back to the **hard drive** to be caught up by the next step. In contrast, **Spark processing** is like the typical one, but holding the results between each step in **memory**. So Hadoop processes data in **hard disk** and Apache Spark in **memory**.
# MAGIC
# MAGIC ![](./assets/map-reduce-processing-2397bafb5a04130d82688f98633d37d8.png )
# MAGIC
# MAGIC **Apache Spark vs Hadoop MapReduce Comparison**
# MAGIC
# MAGIC - **Performance**: Spark is significantly faster than Hadoop MapReduce. This is due to **_Spark’s in-memory data processing_**, which reduces the time it takes to process large datasets by keeping data in RAM. In contrast, MapReduce relies on reading and writing intermediate data to **disk**, which slows down performance considerably.
# MAGIC
# MAGIC - **Ease of Use:** Spark is more **user-friendly** due to its higher-level APIs, which support programming languages like **_Python_**, **_Scala_**, and **_Java_**. It also offers interactive querying, making it easier for developers to work with. **MapReduce** requires more complex code and is harder to program, making it _less accessible to beginners_.
# MAGIC
# MAGIC - **Real-Time Data Processing**: Spark has **real-time stream processing** capabilities, enabling it to handle streaming data as it arrives. This makes it ideal for applications requiring real-time analytics. Hadoop MapReduce, on the other hand, is **batch-oriented** and processes data in large chunks, which limits its ability to handle real-time tasks.
# MAGIC
# MAGIC | Feature | Apache Spark | Hadoop MapReduce |
# MAGIC | --- | --- | -- |
# MAGIC | **Processing Speed** | Faster due to in-memory computation | Slower, relies on disk-based storage |
# MAGIC | **Ease of Use** | Simplified with high-level APIs | More complex, low-level coding required |
# MAGIC | **Real-Time Processing** | Supports real-time stream processing | Limited to batch processing |
# MAGIC | **Fault Tolerance** | Built-in with DAG (Directed Acyclic Graph) | Built-in via task retry mechanism |
# MAGIC | **Language Support** | Supports Scala, Java, Python, and R | Primarily Java |
# MAGIC
# MAGIC **Stream Processing**
# MAGIC
# MAGIC Stream processing involves analyzing data in **real-time** as it's created, enabling immediate actions. Unlike traditional batch processing, which processes data at intervals, stream processing allows **continuous data flows** to be handled instantly. Common applications include _fraud detection_, _IoT analytics_, and _personalized marketing_. 
# MAGIC
# MAGIC Architectures like [**Lambda**](https://www.databricks.com/glossary/lambda-architecture) and Kappa blend real-time and batch processing for more comprehensive insights. As technology advances, **stream processing** is becoming **essential** for real-time data-driven decisions.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orchestration Core Concepts
# MAGIC
# MAGIC But how do we make our pipeline **flow**? 🌊
# MAGIC
# MAGIC * Data Engineering **workflows** often involve **transforming** and **transferring data** from one place to another.
# MAGIC * We want to combine data from different locations, and we want to do this in a way that is **reproducible** and **scalable** when
# MAGIC   there are updates to the data or to our workflows.
# MAGIC * **Workflows** in real-life have multiple steps and stages. We want to be able to **orchestrate** these steps and stages and
# MAGIC   **monitor** the progress of our workflows.
# MAGIC * Sometimes, everything might work fine with just CRON jobs.
# MAGIC * But other times, you might want to control the state transitions of these steps:
# MAGIC   * e.g. If Step A doesn’t run properly, don’t run Step B because the data could be corrupt, instead run Step C.
# MAGIC   * Once again, the concept of [**Directed Acyclic Graphs (DAGs)**](https://en.wikipedia.org/wiki/Directed_acyclic_graph) is helpful
# MAGIC * One popular task orchestration tool is **[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practical Data Workloads
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![big-data-sword.png](./assets/big-data-sword.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC 💡 **REMEMBER:** 
# MAGIC
# MAGIC You will get big data skills, but in reality...
# MAGIC
# MAGIC [Streaming isn’t always the solution](https://www.forbes.com/councils/forbestechcouncil/2024/10/17/the-real-cost-of-real-time-why-streaming-data-isnt-always-the-answer/)! (Optional reading, ~10 minutes)
# MAGIC
# MAGIC There are a bunch of orchestration options, it's just useful to know a few of the names but going into detail is not necessary for the course.
# MAGIC
# MAGIC **DAG-based approaches:**
# MAGIC * [Apache Airflow](https://airflow.apache.org/)
# MAGIC * [Databricks Jobs Orchestration](https://databricks.com/blog/2021/07/13/announcement-orchestrating-multiple-tasks-with-databricks-jobs-public-preview.html)
# MAGIC * [Dagster](https://dagster.io/)
# MAGIC
# MAGIC **Event-Driven + Declarative**
# MAGIC * [Databricks Auto Loader](https://databricks.com/discover/demos/delta-lake-data-integration-demo-auto-loader-and-copy-into)
# MAGIC * [Delta Live Tables](https://databricks.com/discover/demos/delta-live-tables-demo)
# MAGIC
# MAGIC **Other triggers:**
# MAGIC * [AWS Lambda](https://aws.amazon.com/lambda/)
# MAGIC * [Glue Triggers](https://docs.aws.amazon.com/glue/latest/dg/trigger-job.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CAP Theorem
# MAGIC
# MAGIC <!--
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC <figure class="video-container">
# MAGIC     <iframe width="560" height="315" src="https://www.youtube.com/embed/9SSvdLnmDiI" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen="allowfullscreen"></iframe>
# MAGIC </figure>
# MAGIC </div>
# MAGIC -->
# MAGIC
# MAGIC Watch the video **["CAP Theorem Illustrated"](https://www.youtube.com/embed/9SSvdLnmDiI)** by Mark Richards.
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC * **C** (Stands for Consistency)
# MAGIC * **A** (Stands for Availability)
# MAGIC * **P** (Stands for Partition tolerance)
# MAGIC
# MAGIC The CAP theorem states that a distributed system can only **provide two out of three guarantees**: _Consistency_, _Availability_, or _Partition Tolerance_.
# MAGIC
# MAGIC To be successful as a data engineer, you need to understand the data architecture you’re using or designing and the source systems producing the data you’ll need. In addition, it's important to be aware of CAP Theorem and Distributed Systems Architecture. 
# MAGIC
# MAGIC <!--
# MAGIC A **CP system** will say _“Sorry, I can’t be sure yet”_ to the client, in order to avoid giving an out-of-date answer.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![cap.png](./assets/cap.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC An **AP system** tries to spits out an answer even if it might not be the most up-to-date one.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![cap-wikipedia.png](./assets/cap-wikipedia.png)
# MAGIC
# MAGIC Reference [Wikipedia](https://en.wikipedia.org/wiki/CAP_theorem)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC -->
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Architecture Reference
# MAGIC
# MAGIC <!--
# MAGIC <iframe width="560" height="315" src="https://www.youtube.com/embed/Mzz4o2xDVzw" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC -->
# MAGIC
# MAGIC 1. Watch the following [**video**](https://www.youtube.com/embed/Mzz4o2xDVzw) to learn about some reference architectures.
# MAGIC
# MAGIC 1. Read the article [**Modern Big Data Architectures - Lambda & Kappa**](https://luminousmen.com/post/modern-big-data-architectures-lambda-kappa)
# MAGIC
# MAGIC
# MAGIC **⚠️** **IMPORTANT:** **Data architecture** is the design of systems to support the evolving data needs of an enterprise, achieved by flexible and reversible decisions reached through a careful evaluation of **trade-offs**.
# MAGIC
# MAGIC Next up, we have a look at data pipelines and architectural schemas from different cloud providers and open source solutions. While the workflows look very similar, the names of the tools used in each step of the pipeline differ. We don't expect to remember all the technologies and tools used in these examples, but perhaps we can recognize a provider or number of tools we already know and see how they fit into these pipelines.
# MAGIC
# MAGIC #### Typical Data Pipeline using Delta Lake
# MAGIC
# MAGIC ![typical-data-pipeline.png](./assets/typical-data-pipeline.png)
# MAGIC
# MAGIC #### Open Source Example Architecture
# MAGIC The use of the different tools' logos makes this look less refined than some of the cloud provider examples but still resembles a very viable architecture.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![multi-cloud-cloud-agnostic-architecture.png](./assets/multi-cloud-cloud-agnostic-architecture.png)
# MAGIC [Reference](https://towardsdatascience.com/scalable-efficient-big-data-analytics-machine-learning-pipeline-architecture-on-cloud-4d59efc092b5)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC #### AWS Reference Architecture
# MAGIC
# MAGIC The following example uses AWS Glue. There are other AWS services like EMR (Elastic MapReduce) that can handle even more complex cases.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![aws-overview.png](./assets/aws-overview.png)
# MAGIC [Reference](https://aws.amazon.com/blogs/architecture/field-notes-how-to-build-an-aws-glue-workflow-using-the-aws-cloud-development-kit/)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC #### Batch Processing on Azure
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![batch-processing-on-azure.png](./assets/batch-processing-on-azure.png)
# MAGIC [Reference](https://docs.microsoft.com/en-us/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC #### Batch & Streaming (Lambda Architecture) on Azure
# MAGIC It seems somebody wanted to mention as many of Azure's services as they can in this picture 😃
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![batch-streaming-azure-lambda.png](./assets/batch-streaming-azure-lambda.png)
# MAGIC [Reference](https://docs.microsoft.com/en-us/azure/architecture/example-scenario/dataplate2e/data-platform-end-to-end)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC #### Lambda vs. Kappa Architecture
# MAGIC
# MAGIC Read about the [differences](https://luminousmen.com/post/modern-big-data-architectures-lambda-kappa).
# MAGIC
# MAGIC **Lambda**: The two layers (speed layer & batch layer) resemble the two legs of the λ symbol.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![lambda.png](./assets/lambda.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC **Kappa**: The left stem of the 𝚱 (kappa symbol) signifies unified input data store and processing logic.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![kappa.png](./assets/kappa.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC | Feature | Lambda Architecture | Kappa Architecture |
# MAGIC | -- | -- | -- |
# MAGIC | Layers | Batch Layer, Speed Layer, Serving Layer | Speed Layer, Serving Layer |
# MAGIC | Batch processing | Uses a Batch Layer for historical data processing | Eliminates Batch Layer, processes data in real-time |
# MAGIC | Complexity | Higher complexity with separate batch and speed logic | Simpler, single codebase for real-time and historical data |
# MAGIC | Use Cases | Best for applications requiring both real-time and historical data accuracy | Suitable for real-time processing when batch logic isn’t needed |
# MAGIC | Tools | Requires both streaming and batch tools | Single toolset for streaming |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC There was a lot of content to digest in this chapter: From the differences of Spark and MapReduce via the CAP Theorem, different architecture examples towards the Lambda and Kappa architecture, we just enjoyed a wild ride through the vast space of data architectures.
# MAGIC
# MAGIC Next up, we will dive deeper into practical exercises, getting to know different data formats and how to utilize them in Apache Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                    |
# MAGIC |---------------------------------------------------------------|
# MAGIC | <a href="$./2.2 Data Formats" target="_self">Data Formats</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>