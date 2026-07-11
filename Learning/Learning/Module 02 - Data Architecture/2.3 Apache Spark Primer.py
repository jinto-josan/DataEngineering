# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark Primer
# MAGIC
# MAGIC In this lesson you will learn more about Apache Spark, the difference between transformation and actions, and the difference between driver and worker nodes.
# MAGIC You'll also start coding in Python using the Dataframe API.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC
# MAGIC * Understand more details of Apache Spark and the differences of Transformations and Actions
# MAGIC * Code using the Apache Spark Data Frames API
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introduction to Spark
# MAGIC
# MAGIC 1. Read the [**Slides from Brooke Wenig**](https://brookewenig.com/SparkOverview.html#/)
# MAGIC
# MAGIC 1. Watch the following video: **[Apache Spark: Why use Spark?](https://www.youtube.com/embed/35Mjaa1YWTk)**
# MAGIC
# MAGIC <!--
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC <figure class="video-container">
# MAGIC     <iframe width="560" height="315" src="https://www.youtube.com/embed/35Mjaa1YWTk" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen="allowfullscreen"></iframe>
# MAGIC </figure>
# MAGIC
# MAGIC In Databricks you do not see the video: [please visit the original on youtube](https://www.youtube.com/embed/35Mjaa1YWTk)
# MAGIC </div>
# MAGIC -->
# MAGIC
# MAGIC 💡 **Some facts:**
# MAGIC
# MAGIC * Scala is only faster than Python if you’re writing a lot of custom **[UDFs](https://docs.databricks.com/en/udf/index.html)** or data structures with [**RDDs**](https://www.databricks.com/glossary/what-is-rdd). If you’re using **built-in Spark functions**, then performance is **identical**.
# MAGIC
# MAGIC * Most modern Spark users are shifting towards a **Python** codebase to take advantage of modern data science and machine learning tools - see the graphics below for empirical evidence 😉.
# MAGIC
# MAGIC * What is the difference between **Transformations (lazy evaluation)** and **Actions**? Transformations are operations that define a new RDD from an existing one, such as `map()`, `filter()`, `flatMap()`, `groupByKey()`, etc. They are **lazy**, meaning that when you apply a transformation, Spark does not immediately execute the operation. Instead, it creates a logical execution plan that describes how the data should be transformed, but it doesn’t actually compute or process the data until an **Action** is triggered. In contrast, **Actions** (such as `collect()`, `count()`, `foreach(:func)`, etc.) are operations that trigger the actual execution of the transformations you've defined on an RDD. When an action is called, Spark calculates the result by executing all the transformations needed. Actions result in a value being returned to the driver program or saving data to external storage.
# MAGIC
# MAGIC * What is the difference between **Driver** and **Worker nodes**? The **Driver node** is the master node in a Spark application that oversees the whole execution process. It is responsible for coordinating and managing the entire **Spark job**. In contrast, **Worker nodes** are responsible for executing the tasks that the Driver assigns. These nodes perform the actual processing of the data.
# MAGIC
# MAGIC ### Programming language popularity
# MAGIC
# MAGIC Python has massively overtaken Scala in popularity for Apache Spark.
# MAGIC
# MAGIC | 2013 | 2020 |
# MAGIC |--|--|
# MAGIC |![spark-usage-2013.png](./assets/spark-usage-2013.png)|![spark-usage-2020.png](./assets/spark-usage-2020.png)|
# MAGIC
# MAGIC For the exercises of this course we will use Python, the APIs that we will be using are:
# MAGIC * [Apache Spark DataFrames](https://www.databricks.com/spark/getting-started-with-apache-spark/dataframes)
# MAGIC * [Pandas](https://pandas.pydata.org/docs/user_guide/index.html)
# MAGIC
# MAGIC 💡 There is no need to read these documentations up front. Please do keep a reference / bookmark to them in order to quickly find them when needed. Whenever we need specific functions for an exercise, those will be linked to at that exercise.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hands on Apache Spark

# COMMAND ----------

# MAGIC %pip install findspark pyspark
# MAGIC
# MAGIC # And uninstall a few helpers we prepared for you
# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC
# MAGIC # now install the databricks helpers 
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers.git
# MAGIC
# MAGIC # # now install and also the databricks test cases
# MAGIC %pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests.git

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "apache_spark_primer"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# Get the path of the current notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Strip away the last two segments (file name and current folder) to get the root folder of all learning modules
workspace_users_path = "/".join(notebook_path.split("/")[:-2])

print(f"Workspace Users Path: {workspace_users_path}")

# COMMAND ----------

# Prepare the environment
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
input_dir = f"/Workspace/{workspace_users_path}/Module 02 - Data Architecture/data"
output_dir = f"/Workspace/{workspace_users_path}/Module 02 - Data Architecture/output"

print("Input directory: ", input_dir)
print("output directory: ", output_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️** **IMPORTANT:** make sure that you've executed the code from the [**Data Formats**]($./2.2 Data Formats) **Notebook**.

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("My First Spark dataset") \
    .getOrCreate()

animals = pd.read_parquet(f"{input_dir}/animals.parquet")

# We transform the pandas dataframe to a spark dataframe
sparkDF = spark.createDataFrame(animals) 
sparkDF.printSchema()
sparkDF.show()

sparkDF.write.mode("overwrite").format("parquet").save(f"{working_directory}/spark_animals.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC Reading the spark dataframe in

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Animals Dataset") \
    .getOrCreate()

df = spark.read.format("parquet").load(f"{working_directory}/spark_animals.parquet")

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Display the dataframe as an HTML Table.

# COMMAND ----------

from IPython.display import display 

displayHTML(df.display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this chapter, we had a first look at Apache Spark. We created a `SparkSession`, read a file, displayed its contents and saved the file under a different name. We'll go into more details in the next chapter.

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                | Next Topic                                                                                                  |
# MAGIC |---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./2.2 Data Formats" target="_self">Data Formats</a> | <a href="$./2.4 Gentle Introduction to Apache Spark" target="_self">Gentle Introduction to Apache Spark</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>