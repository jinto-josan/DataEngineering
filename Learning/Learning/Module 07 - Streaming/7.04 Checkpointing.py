# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC We now understand that stream processing applications in Spark run in an infinite loop of micro-batches. These applications run on a continuous basis on an unbounded set of real time data. We also know that any application in general cannot run for an infinite amount of time even if we want them to do that, for two practical reasons:
# MAGIC
# MAGIC - Applications often suffer system failures or cluster crashes.
# MAGIC - Systems sometimes need to be brought down for periodic maintenance.
# MAGIC
# MAGIC This means that our applications need to be able to handle stoppages and when they do happen due to the aforementioned reasons, we would want them to recover gracefully. This is where checkpointing comes into picture.
# MAGIC
# MAGIC Spark Checkpointing provides the ability to 'bookmark' a data stream when a streaming application stops and uses the bookmark to pick up where the application left off when the application is restarted. It accomplishes that by maintaining a checkpoint location either locally or remotely (more on local/remote bit in the later sections). Spark checkpoints comprise two key items :
# MAGIC
# MAGIC - **Read position** - the start and end date range of the current micro-batch.
# MAGIC - **State information** - the state that applications would want to maintain across micro-batches, when working with Stateful Streams.
# MAGIC
# MAGIC ## Checkpointing Requirements
# MAGIC
# MAGIC Checkpoints in Spark can be leveraged to their full benefit if we fulfill the following requirements:
# MAGIC
# MAGIC - Reliably update the checkpoint location when new data has been processed (starting an application from an old location will result in duplicate data processing)
# MAGIC - Use a source (e.g. File Source, Kafka Source) from which data can be replayed (this is useful for incomplete data in a certain micro-batch).
# MAGIC - Processing logic is consistent and idempotent (the same result is obtained when given the same input data)
# MAGIC
# MAGIC Let us now look at an example of how checkpoints are actually created. We will use the same Stateful Streaming Wordcount code from previous section and add checkpointing code to it. Infact this is something that we have already been doing in the previous examples, specially while writing the data to the output stream, in this line of code
# MAGIC
# MAGIC `.option("checkpointLocation", f"{working_directory}/checkpoint")`
# MAGIC
# MAGIC With this understanding now, let us try a few things in the below code from the checkpointing perspective. We will be using the stateful code for the previous sections for this purpose

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "checkpointing"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType


# Variable that will be used to create a DataFrame
greeting = "Top of the morning"

# Schema declaration
schema = StructType([
  StructField("greeting", StringType(), True)
])

# Let us create a batch dataframe
df = spark.createDataFrame(data = [(greeting,)], schema=schema)

# Display it
display(df)

# Write it to the disk (DBFS based cloud)
df.write.mode("overwrite").format("parquet").save(f"{working_directory}/batch")


# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

linesDF = (spark.readStream
      .option("header", "True")
      .format("parquet")
      .option("path", f"{working_directory}/batch")
      .schema(schema)
        .load())

# Lets add some transformation logic
# Split the lines into words
wordsDF = (linesDF
         .select(
           explode(split(linesDF.greeting, " "))
           .alias("word")))

# Let us generate a running word count
wordCountsDF = wordsDF.groupBy("word").count()

query = (wordCountsDF
         .writeStream    
         .outputMode("complete") 
         .option("checkpointLocation", f"{working_directory}/checkpoint") 
        #  .option("path", f"{working_directory}/streaming")
         .format("console")
         .start())




# COMMAND ----------

# MAGIC %md
# MAGIC Here are the steps you can follow to see the checkpointing in action :-
# MAGIC
# MAGIC - Run the code in Cells 2, 3 and 4 above to do the set up for this exercise.
# MAGIC - Once done, run the code in Cell 5 to create a batch dataframe in parquet format.
# MAGIC - Please run the code in cell 6 now to get the streaming program up and running. Once the program us running, just to double check, you can go to the `Catalog` link from the side bar and go to the `Browse DBFS` page and check if the `batch` and `checkpoint` folders are created in your `/FileStore/<username>/checkpointing` directory in DBFS.
# MAGIC - Now go to the `Compute` link on the sidebar, click on your cluster, go to `Driver Logs` and then to `Standard Ouput`. You should be able to see the output of the dataframe here, just like you did in the `Stateful vs Stateless` notebook.
# MAGIC - Now stop your steaming program in Cell 6 above. You can do that by clicking on the `Interrupt` button on the top left hand side of the same cell. This not only stops the streaming program but also checkpoints the data from state store (more on this in the next section) on the disk. This helps your program to pick up the data already read from the previous streams when it starts again
# MAGIC - Now restart the streaming program again by executing the code in Cell 6. This will start your streaming program again. More importantly, your streaming program would have read the data from the checkpoint, thought it will not show in the standard output yet.
# MAGIC - Now run the code in Cell 5 to create the batch data again in batch folder.
# MAGIC - This will be immediately picked up by the running streaming program in Cell 6, and after a while, you should be able to see the updated and aggregated streaming dataframe in the Standard output of the cluster (i.e the word `Top` as 2, `of` as 2 etc)
# MAGIC - This clearly demonstrates that the streaming program in reading the data from the checkpointing location on restarts and then carries on with the business as usual !

# COMMAND ----------

# MAGIC %md
# MAGIC Not to forget, cleaning up your working directory and stopping the below by runnning the code below

# COMMAND ----------


helpers.clean_working_directory()

query.stop()


# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                      | Next Topic                                                                |
# MAGIC |-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
# MAGIC | <a href="$./7.03 Stateful vs Stateless Streaming" target="_self">7.03 Stateful vs Stateless Streaming</a> |  <a href="$./7.05 Handling Late Data" target="_self">7.05 Handling Late Data</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>