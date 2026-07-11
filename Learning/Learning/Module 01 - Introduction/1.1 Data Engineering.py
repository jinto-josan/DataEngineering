# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering
# MAGIC
# MAGIC **Data Engineering** is an amazing and huge field. You will find a lot of technologies in the [ecosystem](https://mad.firstmark.com/) which by the way, is also known as **Data Milky Way** . It's important to understand some foundational concepts, principles and keywords before jumping into a technology. So here are the learning objectives of this module 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Explain, in your words, what data engineering is
# MAGIC * Get familiar with some terminology and buzzwords used in the field
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 20 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Data Engineering Works
# MAGIC
# MAGIC Watch the following [video](https://www.youtube.com/watch?v=qWru-b6m030) to get an overall introduction of what data engineering is.
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC - **Data engineering** is the development, implementation, and maintenance of systems and processes that take in raw data and produce high-quality, consistent information that supports downstream use cases, such as analysis and machine learning.
# MAGIC
# MAGIC - **Data Warehouse:** is a data repository where you have structured and historical data for a business domain, this data is consumed to analyze.
# MAGIC
# MAGIC - **Data Lake:** is a data repository where you have structured and non-structured data (videos, images, text, etc...)
# MAGIC
# MAGIC - **Cloud Data Warehouse:** some cloud vendors have evolved data warehouses offering highly scalable computing and storage capabilities and storage in the cloud. Some examples are: [_Amazon Redshift_](https://aws.amazon.com/redshift/), [_Google Big Query_](https://cloud.google.com/bigquery), [_Azure Synapse_](https://azure.microsoft.com/en-us/products/synapse-analytics)
# MAGIC
# MAGIC - **ETL:** stands for _Extract - Transform - Load_, it's a pattern to ingest data commonly used to load data into Data Warehouses.
# MAGIC
# MAGIC - **ELT:** stands for _Extract - Load - Transform_, it's a modern pattern to ingest data and commonly used to load data into Data Lakes.
# MAGIC
# MAGIC - **Data Pipeline:** is the combination of architecture, systems, patterns, and processes that moves and transforms data to the desired destinations (Data Warehouse, Data Lake or others).
# MAGIC
# MAGIC - **Big Data:** mainly refers to the volume or data size (_Terabytes, Petabytes, Exabyte, Zetabytes_) that needs to be processed and analyzed. Thanks to modern technologies and highly scalable computing power now we are able to process and analyze it. This has become an integral part of data engineering.
# MAGIC
# MAGIC - **Batch and Streaming**: are ingestion processes used to move the data to the required destinations (Data Warehouse, Data Lake, and so on). Batch usually ingests the data in a daily frequency. Streaming ingests the data in real-time, once it's generated at the source.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                        | Next Topic                                                    |
# MAGIC |-----------------------------------------------------------------------|---------------------------------------------------------------|
# MAGIC | <a href="$./1.0 Table of Contents" target="_self">Table of Contents</a> | <a href="$./1.2 Data Roles" target="_self">Data Roles</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>