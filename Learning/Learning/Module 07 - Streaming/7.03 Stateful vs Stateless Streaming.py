# Databricks notebook source
# MAGIC %md
# MAGIC ## Two Types of Streaming Transformations
# MAGIC
# MAGIC There are two types of transformation that can be done in Spark Structured Streaming known as Stateless and Stateful transformation. Our code can have both and, in fact, both types of transformations can work in tandem with each other. First, let us take a look at the stateless example below :

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests


# COMMAND ----------

exercise_name = "stateful_vs_stateless"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have the basic set up in place, lets create a batch dataframe with some dummy data. This data is going to get consumed by our stateless streaming program that will come just below it

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

# MAGIC %md
# MAGIC Now comes the streaming code. The code below starts with reading the data from file input source and stores the reference of the resulting streaming dataframe in a `linesDF` variable. 
# MAGIC
# MAGIC There are a number of input sources available in spark available at this [link](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources). 
# MAGIC
# MAGIC The code then uses `explode` and `split` transformations to create another dataframe with a single column called `words`. The reference to this dataframe is saved in a `wordsDF` variable.
# MAGIC
# MAGIC Lastly, the `wordsDF` is written to the writeStream code in the last few lines of the code below.

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# Schema declaration
schema = StructType([
  StructField("greeting", StringType(), True)
])

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

query = (wordsDF
         .writeStream
        #  .format("parquet")     
         .outputMode("append") 
         .option("checkpointLocation", f"{working_directory}/checkpoint") 
        #  .option("path", f"{working_directory}/streaming")
        .format("console")
         .start())




# COMMAND ----------

# MAGIC %md
# MAGIC So now we are all set to run the above stateless streaming code and see the output. Here are the steps you can follow
# MAGIC
# MAGIC - Please run the code in cells 5, 6 and 7 above to create a working directory.
# MAGIC - Now run the code in cell 9 above to create a batch (i.e Spark SQL) dataframe. This should give you the output given in `greeting` variable on line 9 in the same cell. (Please enable line numbers for your notebooks from `View` menue and `Line number` option from above). Feel free to change the value of `greeting` as per your liking. 
# MAGIC - Once done, move to cell 11 which has your Stateless Spark Streaming code written. Hit the run button of that cell to run the streaming program. This should start the streaming program which will be running continously to read the data from `batch` folder created in cell 9
# MAGIC - To see the output from our streaming program, on the sidebar, go to `compute` link and open it a new tab by right clicking on it. This should open up your workspace cluster page. Click on the cluster and go to `Driver logs` section. You will see a sub-section here by the name `Logs - Recent log files` and `Standard Output` showing the logs of your cluster.
# MAGIC - You should be able to see the output of your streaming program here `Top of the morning` (or anything else that you might have specified in your `greeting` variable in cell 9)
# MAGIC - Try running the batch program in cell 9 again to reproduce the data. Your data should show up again in the logs.
# MAGIC - Please note that since this is a `Stateless Streaming` the data of your previous runs in not stored in the program. This should change as we move towards a similar kind of `stateful streaming program`

# COMMAND ----------

# MAGIC %md
# MAGIC Lets clean up our working directory below to get ready to run the next program

# COMMAND ----------


helpers.clean_working_directory()


# COMMAND ----------

# MAGIC %md
# MAGIC also, not to forget to stop our running stateless streaming program now that we dont need it anymore.

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC So we are all set now to start writing our `Stateful streaming program`. This program should largly remain the same with just few additional lines of code. Let us quickly check that below

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
         .format("console")
         .start())




# COMMAND ----------

# MAGIC %md
# MAGIC So just like our stateless program, this program is also reading data from the batch folder, and explodes and splits it into a `WordsDF` dataframe from lines 1 to 16. Then there a new lines of code here on line 19, which `groupsBy` the `words` column and count the number of occurances of each values in the column to save it into a new dataframe called `wordCountsDF`. Lastly, the `wordCountsDF` is written in the output stream to show up on the console by using format as `console`. You will also note that the output streaming has a changed output format as `complete`. That is because if our program does any type of aggregation (like in line 19 above), only the `complete` output mode works. 

# COMMAND ----------

# MAGIC %md
# MAGIC Lets look at the steps of running this program as follows (this would again be largly same as the stateless program with only the changed output). Lets quickly check it below
# MAGIC - Run cell 4 and 5 again to create the working directory.
# MAGIC - Run the cell 7 again to create data in the `batch` folder, ready to be read by our streaming program
# MAGIC - Now run the cell 16 and check the output in your cluster `Standard Output` again in the DriverUI. This should give you the dataframe with aggregated output of words (essentially our `wordcount`)
# MAGIC - Run the program again in Cell 7 and check the output again in the `Standard Output`. You should be able to see the aggregated word count from your previous run from cell 9 and the current run.
# MAGIC
# MAGIC Now let us clean up our working directory and stop the streaming query once again to free up the resources.

# COMMAND ----------


helpers.clean_working_directory()

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                            | Next Topic                                                      |
# MAGIC |-------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
# MAGIC | <a href="$./7.02 Spark Structured Streaming" target="_self">7.02 Spark Structured Streaming</a> | <a href="$./7.04 Checkpointing" target="_self">7.04 Checkpointing</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>