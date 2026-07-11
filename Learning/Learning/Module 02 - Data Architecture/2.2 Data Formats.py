# Databricks notebook source
# MAGIC %md
# MAGIC # Data Formats
# MAGIC
# MAGIC In the previous chapter, we had a look at the big picture: different architectures, several cloud providers and the lambda and kappa architectures. As every journey starts with the first step, we will now build upwards, starting with a closer look at data formats.
# MAGIC
# MAGIC In this lesson you will learn about the most common formats used in data engineering, you'll recap about data serialization and deserialization, and you'll be able to practice reading and writing these formats in your cluster.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Know the differences of the popular formats such as CSV, JSON, Avro and Parquet
# MAGIC * Write Python code to read and write CSV formats, Excel Formats, JSON and Parquet
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️** **IMPORTANT:** Before you start the practical exercise, make sure that you start your cluster and connect this workbook to it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Serialization & Data Formats
# MAGIC #### Wait...what’s data serialization again?
# MAGIC (extracted from [Devopedia](https://devopedia.org/data-serialization))
# MAGIC
# MAGIC * Data serialisation is the process of converting data objects present in complex data structures into a byte stream for storage, transfer and distribution purposes on physical devices.
# MAGIC
# MAGIC * Computer systems may **vary** in their hardware architecture, OS, addressing mechanisms. Internal representations of data also vary accordingly in every environment/language. Storing and exchanging data between such varying environments requires a **platform-and-language-neutral data format** that all systems understand.
# MAGIC
# MAGIC * Once the serialized data is transmitted from the source machine to the destination machine, the reverse process of creating objects from the byte sequence called **deserialization** is carried out. Reconstructed objects are clones of the original object.
# MAGIC
# MAGIC * Choice of data serialization format for an application depends on factors such as data complexity, need for human readability, speed and storage space constraints.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![data-serialisation.png](./assets/data-serialisation.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### Data Formats
# MAGIC Data Serialisation and Data Formats go hand in hand because the characteristics of the format determine how data can be stored and retrieved which in turn has an impact on performance, structure and size (in transport and at rest). Based on these patterns on storage and access, we might choose a specific file format to optimise those processes. There are a variety of file formats common to Data Engineering use cases - the classics include **CSV**, **JSON** (i.e. [JSON Lines](https://jsonlines.org/)), **Avro**, and **Parquet**. There are other more modern data formats which we'll get to a little later.
# MAGIC
# MAGIC A few important points to note before we dive into individual formats:
# MAGIC * Data on the hard disks is saved in blocks and gets loaded one at a time in memory.
# MAGIC * Reading unnecessary, fragmented and random data is expensive therefore sequential reads / writes are recommended.
# MAGIC * File formats determine the way that data is stored on the disk and is of different types for e.g Unstructured (Text, CSV, TSV), Semi-structured (JSON, XML) and Structured (Avro, Parquet).
# MAGIC * File formats stores data in a row oriented, column oriented or a hybrid format. Irrespective of the data orientation, the data is always arranged on the disk in sequential manner due the benefit of faster reads as mentioned above.
# MAGIC * **Row-wise formats** are best for **write-heavy operations** (**OLTP** workflows), Columnar are best for **read-heavy** (**OLAP** workflows) and Hybrid Formats try to provide the best of both worlds.
# MAGIC
# MAGIC Some helpful vocabulary:
# MAGIC * **Splittable** = take one file and split it into multiple chunks (partitions) to allow for concurrent processing
# MAGIC * **Compressibility** = encoding a file using fewer bits than the original representation
# MAGIC * **Self-describing** = file contains both data and metadata (about the data, like the schema)
# MAGIC * **Schema evolution** = file can handle new columns and backwards-compatibility
# MAGIC
# MAGIC ###### CSV
# MAGIC * Row-based and human-readable
# MAGIC * Compressible and splittable
# MAGIC * Faster writes but slower reads (depending on storage and file size)
# MAGIC * Flexible (as in, data can be easily changed, but can also be corrupted) but does not support schema evolution
# MAGIC * Best suited for smaller data sets which don't need any query time optimizations
# MAGIC
# MAGIC ###### JSON
# MAGIC * Row-based and human-readable
# MAGIC * Compressible and splittable
# MAGIC * Faster writes but slower reads
# MAGIC * Self-describing
# MAGIC * Support complex data type like arrays and nested values
# MAGIC * Schema Evolution is not supported though column names are embedded inside the datasets
# MAGIC * Best suited for if the dataset has highly nested values which, like CSV, does not need query optimizations
# MAGIC * Suited for smaller data sets if used as a data exchange format
# MAGIC
# MAGIC ###### Avro
# MAGIC * Row-based but not human-readable
# MAGIC * is itself a serialization format so it can be used for data exchange across the wire across different OS
# MAGIC * Self describing with support for Schema Evolution
# MAGIC * Binary format with Schema stored inside the file in JSON format
# MAGIC * Optimised for write-intensive applications
# MAGIC * Compressible and splittable
# MAGIC * Supports rich data structures like arrays and enumerated types
# MAGIC * Best suited for both realtime and batch workflows where fast data writes and transfer, schema validation and evolution is required.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![avro-recap.png](./assets/avro-recap.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ###### Parquet
# MAGIC * Columnar-based and not human-readable
# MAGIC * Self-describing
# MAGIC * Hybrid format where rows are grouped by row groups and then columns are partitioned
# MAGIC * Binary format like Avro with Schema stored inside the file in JSON format but does not support Schema Evolution
# MAGIC * Optimised for **read intensive applications** but performs well for write operations too due to its **hybrid** nature
# MAGIC * Compressible and splittable
# MAGIC * Support rich data structures like arrays and enumerated types, again like Avro
# MAGIC * Best suited for **analytics workflow** where fast query performance is needed
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![parquet-columnar-storage.png](./assets/parquet-columnar-storage.png)
# MAGIC
# MAGIC ![parquet-columnar-storage-for-the-people.png](./assets/parquet-columnar-storage-for-the-people.png)
# MAGIC
# MAGIC ![parquet-access-only-data-you-need.png](./assets/parquet-access-only-data-you-need.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ###### Additional References
# MAGIC It's optional content, but it will help you to gain a deeper understanding of this topic.
# MAGIC
# MAGIC * [**Big Data File Formats**](https://luminousmen.com/post/big-data-file-formats)
# MAGIC * [**Comparing the formats**](https://www.datanami.com/2018/05/16/big-data-file-formats-demystified/#:~:text=The%20biggest%20difference%20between%20ORC,in%20a%20row%2Dbased%20format.&text=While%20column%2Doriented%20stores%20like,might%20be%20the%20better%20choice.)
# MAGIC * [**Big Data File Showdown: Avro vs Parquet**](https://www.confessionsofadataguy.com/big-data-file-showdown-avro-vs-parquet-with-python/)
# MAGIC * [Avro is popular choice for streaming](https://www.confluent.io/blog/avro-kafka-data/) and persisting streaming data into data lakes (e.g. [Azure Event Hubs Capture](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview#exploring-the-captured-files-and-working-with-avro)).
# MAGIC * [The Apache Spark File Format Ecosystem](https://www.youtube.com/embed/auNAzC3AU18?start=153)
# MAGIC * [The Parquet Format and Performance Optimization Opportunities Boudewijn Braams (Databricks)](https://www.youtube.com/watch?v=1j8SdS7s_NY)
# MAGIC
# MAGIC <!--
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC     <figure class="video-container">
# MAGIC         <iframe width="560" height="315" src="https://www.youtube.com/embed/auNAzC3AU18?start=153" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen="allowfullscreen"></iframe>
# MAGIC     </figure>
# MAGIC Big Data File Formats
# MAGIC </div>
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC <figure class="video-container">
# MAGIC     <iframe width="560" height="315" src="https://www.youtube.com/embed/1j8SdS7s_NY" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen="allowfullscreen"></iframe>
# MAGIC </figure>
# MAGIC
# MAGIC In Databricks you do not see the video: please visit the [original on youtube](https://www.youtube.com/embed/1j8SdS7s_N)
# MAGIC
# MAGIC A Deeper Dive into Parquet + Performance Optimisation
# MAGIC
# MAGIC </div>
# MAGIC -->
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC Unlike MapReduce vs Spark, there’s no clear winner. There’s always still a time and place for each of these formats!
# MAGIC
# MAGIC | | CSV | JSON | Parquet | Avro |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC |Compressibility | ☑️ | ☑️ | ☑️ | ☑️ |
# MAGIC |Human Readability | ☑️ | ☑️ | | |
# MAGIC |Schema Evolution | | | | ☑️ |
# MAGIC |Row or Columnar Storage | | | ☑️ | ☑️ |
# MAGIC
# MAGIC ##### Delta Lake
# MAGIC
# MAGIC We'll cover Delta Lake in detail in the following sections of this course, but let's define it here so you can keep an ear out for it. Delta Lake is an **open source** storage layer. It is similar to Parquet (some people refer to it as **Parquet Plus**) but it provides ACID transactions, time travel, the ability to remove data across various versions (vaccuuming), and you can stream and batch at the same time.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ **Disclaimer about using Databricks AI + Autocompletion**
# MAGIC >
# MAGIC > Databricks offers its own version of a coding assistant for both line completion and on the right side of your editor for a chatbot-like approach.
# MAGIC >
# MAGIC > As many large language models (LLMs), the assistant makes suggestions with much confidence, regardless of its accuracy.
# MAGIC > This means: For many syntax related topics this assistance works really well, for others it might offer suggestions that lead into the wrong direction.
# MAGIC >
# MAGIC > If you want to have a greater challenge for the entire course or the assistant is getting your nerves, you can disable it in your user settings.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's deal with some data 
# MAGIC Assume we have some data in a specific format. Let's read them and convert them to something else.
# MAGIC
# MAGIC In the next few cells we'll read the file `./data/dogs.csv` that is in CSV format and write the data to Excel. We are using Pandas to read and write the data.

# COMMAND ----------

# Required for writing dataframe to excel
%pip install pandas openpyxl pyarrow

# And uninstall a few helpers we prepared for you
%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests

# now install the databricks helpers 
%pip install git+https://github.com/data-derp/databricks_helpers.git

# # now install and also the databricks test cases
%pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests.git

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "data_formats"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
current_working_user = helpers.current_working_user()
print(f"Your user current working user is: {current_working_user}")

# COMMAND ----------

# Get the path of the current notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Strip away the last two segments (file name and current folder) to get the root folder of all learning modules
workspace_users_path = "/".join(notebook_path.split("/")[:-2])

print(f"Workspace Users Path: {workspace_users_path}")

# COMMAND ----------

input_dir = f"/Workspace{workspace_users_path}/Module 02 - Data Architecture/data"
output_dir = f"/Workspace{workspace_users_path}/Module 02 - Data Architecture/output"

print("Input directory: ", input_dir)
print("output directory: ", output_dir)

# COMMAND ----------

import pandas as pd
from IPython.display import display


# Read the csv file into a pandas data frame
df = pd.read_csv(f"{input_dir}/dogs.csv")
print(f"Count rows: {len(df)}")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC You just did your first operations on the dataframe: `len` to get the count and `display` to visualize the content.
# MAGIC
# MAGIC Now, we write the dataframe back to disk.

# COMMAND ----------

df.to_excel(f"{output_dir}/dogs.xlsx", sheet_name='Dogs')

# COMMAND ----------

# MAGIC %md
# MAGIC Do you see the Excel file inside the output directory?

# COMMAND ----------

# MAGIC %md
# MAGIC Ok, there is another CSV file in the data directory named `cats.csv`. 
# MAGIC Please read this file too, add the kind of the animal (**dog** or **cat**) to the rows in each dataframe, combine the dataframes into one, and write it to **one single** Excel file.
# MAGIC
# MAGIC Hint: the `concat` function (see [documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html)) of Pandas is very handy for this task.

# COMMAND ----------

df_dogs = pd.read_csv(f"{input_dir}/dogs.csv")
df_cats = pd.read_csv(f"{input_dir}/cats.csv")

### add the kind of the animal to the two data frames 
df_dogs['kind'] = "dog"
df_cats['kind'] = "cat"

# Concat the to data frames into one
### Put your code here.
df_animals = pd.concat([df_dogs, df_cats], ignore_index=True)

df_animals.to_excel(f"{output_dir}/animals.xlsx", sheet_name='All Animals')
display(df_animals)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the to JSON format
# MAGIC You may want to read to [to_json documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html) first.

# COMMAND ----------

# Write the data frame into a json file
### Put your code here.
df_animals.to_json(f"{output_dir}/animals.json")
with open(f"{output_dir}/animals.json") as f: s = f.read()
print(s)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the to PARQUET format

# COMMAND ----------

df_animals.to_parquet(f"{output_dir}/animals.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap
# MAGIC - Data serialization and deserialization: You have read how data is converted to byte stream (serialization) and how data is recovered from it (deserialization) to enable platform and language-neutral transmission.
# MAGIC - You have become familiar with the common data formats used in data engineering.
# MAGIC - You know the difference between the CSV, JSON, Avro and Parquet formats.
# MAGIC - You now know that Delta Lake is based on Parquet.
# MAGIC - You can read and write various data formats with Pandas
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                          | Next Topic                                                                  |
# MAGIC |-------------------------------------------------------------------------|-----------------------------------------------------------------------------|
# MAGIC | <a href="$./2.1 Data Architecture" target="_self">Data Architecture</a> | <a href="$./2.3 Apache Spark Primer" target="_self">Apache Spark Primer</a> |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>