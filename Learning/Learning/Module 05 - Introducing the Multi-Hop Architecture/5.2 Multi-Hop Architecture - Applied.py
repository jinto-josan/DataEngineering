# Databricks notebook source
# MAGIC %md
# MAGIC # Architecture application to EV Domain
# MAGIC We would like you to recall that the **Multi-Hop/Medallion Architecture** comprises of 3 stages: **Bronze**, **Silver**, and **Gold** as below :
# MAGIC
# MAGIC ![](./assets/bronze-silver-gold.png)
# MAGIC Source: [Databricks](https://www.databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC So in this notebook, we will talk about how to take our EV Domain questions (that we have been asking across module 3 and 4) and organise what the input and outputs would be of each of hops as per the above architecture.
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 10 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying the domain
# MAGIC
# MAGIC Let us take a moment and summarize domain questions that we started asking in module 3 and 4 onwards and the steps we have taken to answer them :
# MAGIC
# MAGIC | **When was the last connection time of a Charge Point?** | **What is the total charge time and final charge dispensed for every completed transaction?** |
# MAGIC | --------                        | --------|
# MAGIC |       Read Data with a Schema   |       Read OCPP Data (`data.csv`) |
# MAGIC |       Convert the timestamp     |     Return only StopTransaction Requests (`filter`)    |
# MAGIC |       Finding most recent message per charge point     |  Unpack JSON in StopTransaction Request (`withColumn`, `from_json`)       |
# MAGIC |       Clean up     |   Unpack StartTransaction Response (`withColumn`, `from_json`)      |
# MAGIC |            |   Unpack StartTransaction Request (`withColumn`, `from_json`)      |
# MAGIC |            |   Find the matching StartTransaction Requests (`inner join`)      |
# MAGIC |            |   Join Start and Stop data (`left join`)      |
# MAGIC |            |   Convert the start_timestamp and stop_timestamp fields to timestamp type (`to_timestamp`, `withColumn`)      |
# MAGIC |            |   Calculate the Charge Transaction Duration (total_time) (`withColumn`, `cast`, `maths`)      |
# MAGIC |            |   Calculate total_energy (`withColumn`, `cast`)      |
# MAGIC
# MAGIC
# MAGIC
# MAGIC **NOTE:** There are some **non-business case transformations** that could be relevant to both, the ones listed above or the upcoming ones. Or they could just good curation/cleanup activities in preparation for further transformation. We have tried to list down some of them below
# MAGIC
# MAGIC - Read from a data source (in this case a CSV)
# MAGIC - Unpack from string to JSON
# MAGIC - Flatten (unpack nested structures and bring to top level)
# MAGIC - Casting ambiguous columns to types
# MAGIC
# MAGIC Thinking back to the multi-hop (**Bronze, Silver, Gold**) architecture we can re-organise our transformations into the following stages:
# MAGIC
# MAGIC | **Bronze**                                    |**Silver**|
# MAGIC | --------                                      | --------|
# MAGIC |       Read from data source (raw) and store   | Unpack from string to JSON |
# MAGIC |                                               | Flatten (unpack nested structures and bring to top level) |
# MAGIC |                                               | Casting ambiguous columns to types |  
# MAGIC
# MAGIC And of course, the Gold transformation Step will feature various transformations required to following domain questions
# MAGIC
# MAGIC | **Gold** |
# MAGIC | --------                                    | 
# MAGIC | Last Connection Time                        |
# MAGIC | Find all StopTransaction (`filter`)           |
# MAGIC | Join with StartTransaction Responses, matching on transaction_id (`left join`)           |
# MAGIC | Find the matching StartTransaction Requests (`inner join`)           |
# MAGIC | Calculate the total_time (`withColumn`, `cast`, `maths`)           |
# MAGIC | Calculate total_energy (`withColumn`, `cast`)|
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                        | Next Topic                                                              |
# MAGIC |-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
# MAGIC | <a href="$./5.1 Introduction" target="_self">Introduction</a> | <a href="$./5.3 Exercise - Bronze" target="_self">Exercise - Bronze</a>  |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>