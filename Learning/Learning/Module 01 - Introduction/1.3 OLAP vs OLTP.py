# Databricks notebook source
# MAGIC %md
# MAGIC ## OLAP vs OLTP
# MAGIC
# MAGIC Up until now, you have learnt about data engineering and the common roles that you find in a data team. In this lesson you'll get a better understanding of whether the datastore is **OLTP** or **OLAP** in nature.\
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Know the differences between OLTP and OLAP.
# MAGIC * Get and understanding that when we are dealing with a data project or product we are mainly referring to OLAP-style workloads.
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Milky Way: OLTP vs. OLAP
# MAGIC
# MAGIC 1. Watch the following [**video**](https://www.youtube.com/watch?v=8VD2qoebacA) where you'll understand the differences between OLTP and OLAP.
# MAGIC
# MAGIC 2. Read the following article "**[Difference Between Data Warehouse and Database](https://panoply.io/data-warehouse-guide/the-difference-between-a-database-and-a-data-warehouse/)**".
# MAGIC
# MAGIC 3. Read the following article "**[Data Lake vs. Data Warehouse - Working Together in the Cloud](https://panoply.io/data-warehouse-guide/data-warehouse-vs-data-lake/)**".
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC **OLTP vs OLAP**
# MAGIC
# MAGIC |   | **OLTP - Online Transactional Processing** | **OLAP - Online Analytical Processing** | 
# MAGIC |---| ---  | ---  |
# MAGIC | _Purpose_ |support daily transactions|report and analyze data|
# MAGIC | _Design_ |application-oriented|subject-oriented|
# MAGIC | _Data_ |up-to-date, operational|consolidated, historical|
# MAGIC | _Size_ |Gigabytes|Terabytes, Exabytes, Pettabytes|
# MAGIC | _Queries_ |simple transactions and frequent updates|complex, aggregated queries and limited updates|
# MAGIC | _Users_ |thousands|hundreds|
# MAGIC | _Tasks Examples_ |Update the price of a book|Get the books with best profit margin|
# MAGIC
# MAGIC **Database vs Data Warehouse**
# MAGIC
# MAGIC |   | **Database** | **Data Warehouse** | 
# MAGIC |---| ---  | ---  |
# MAGIC | _Processing Method_ |OLTP|OLAP|
# MAGIC | _Frequent operations_ |DELETE, INSERT, UPDATE quickly in transactions|SELECT and analyze massive volumes of data|
# MAGIC | _Data Structure_ |Highly normalized data|Denormalized|
# MAGIC | _Data Timeline_ |Current, real-time data|Historical|
# MAGIC | _Data Analysis_ |Slow and painful due to the large number of table joins|Analysis is fast and easy due to the small number of table joins|
# MAGIC
# MAGIC **Data Warehouse vs Data Lake**
# MAGIC
# MAGIC |   | **Data Warehouse** | **Data Lake** | 
# MAGIC |---| ---  | ---  |
# MAGIC | _Data_ |structured, processed|raw, structured, non-structured, semi-structured|
# MAGIC | _Processing_ |schema-on-write|schema-on-read|
# MAGIC | _Storage_ |expensive for large data volumes|designed for low-cost storage|
# MAGIC | _Agility_ |less agile, fixed configuration|configure and reconfigure as needed|
# MAGIC | _Security_ |Mature|Maturing|
# MAGIC | _Users_ |Business Professionals|Data Scientists, ML Engineers|
# MAGIC
# MAGIC
# MAGIC - Organizations use Data Warehouses and Data Lakes to **store**, **manage** and **analyze data**
# MAGIC
# MAGIC - **Data Lake:** A data lake is a highly scalable storage system that holds **structured** and **unstructured** data in its original form and format. A data lake does not require planning or prior knowledge of the data analysis - it assumes that analysis will happen later, on-demand.
# MAGIC
# MAGIC - **Data Warehouse:** Traditional data warehouses use a process called **_Extract-Transform-Load (ETL)_**. Data is meticulously mapped from the original data sources to tables in the Data Warehouse, and undergoes transformations to achieve a structured format, to enable reporting and data analysis.
# MAGIC
# MAGIC
# MAGIC **⚠️** **IMPORTANT:** When people/customers talk about developing **Data & AI**, **Analytics**, **Data Science**, **Data Engineering**, **Machine Learning**, they’re most likely referring to **OLAP-style workloads**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                              | Next Topic                                                          |
# MAGIC |-------------------------------------------------------------|---------------------------------------------------------------------|
# MAGIC | <a href="$./1.2 Data Roles" target="_self">Data Roles</a>   | <a href="$./1.4 A Brief History" target="_self">A Brief History</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>