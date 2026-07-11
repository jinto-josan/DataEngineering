# Databricks notebook source
# MAGIC %md
# MAGIC # Data Platforms
# MAGIC
# MAGIC In this lesson, you will gain a foundational understanding of data platforms, their architecture, and the components that support data storage, access, transformation, and consumption. By exploring data platform elements such as self-service tooling, data governance, and the FAIR principles, you will learn how to create accessible, high-quality, and reusable data products.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Understand Data Platforms and Components
# MAGIC * Apply FAIR principles to data handling
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basics of a Data Platform
# MAGIC When answering multiple questions in a domain, it might be helpful to consolidate and share efforts so that other people who have a similar goal can benefit from your work. That can be at the **infrastructure level**, where a lightweight team might be focused on providing tooling to handle _event streams, data storage, data catalogues_ (find your data), and additional common developer tooling (like _libraries, compute power like  Spark_) that are relevant to consumers of the platform. At the level of **data processing**, teams working within the Platform might **develop products that transform the data** that can (a) answer a domain question (using varying degrees of Intelligence) to **make a business decision** or (b) **eliminate the complexity** of a certain data transformation that requires deep domain knowledge and serves that transformed data to another team for further processing.
# MAGIC
# MAGIC #### Components of a Data Platform
# MAGIC ##### Self-Service Platform
# MAGIC A lightweight platform **enables** teams on the platform to both **find and access data** themselves, lowering the barrier to entry, and provides common tooling to get teams started quickly with experimentation. Several offerings can include:
# MAGIC
# MAGIC * Event Streaming Technology
# MAGIC * Data Storage in various forms
# MAGIC * Data Catalogue to help finding data
# MAGIC * ML and Data Engineer tooling (Spark, other cloud-native Data-services)
# MAGIC * Common libraries or templates to speed up the process of experimentation
# MAGIC * Data Lineage tooling
# MAGIC
# MAGIC ##### Data Products
# MAGIC A **Data Product** aims to answer a specific domain question to help a **business make a decision** or **transform data according to a complex domain** construct so that others can reuse the output shape of data. Data Products can be in the form of a simple data transformation or more complex ML models. These are owned by **cross-functional teams** who very closely involve the **Consumer** or **Decision-maker** and employ many Software Development sensible defaults like iterative development, deploying small changes, testing, and continuous deployment.
# MAGIC
# MAGIC ##### Lightweight Data Governance
# MAGIC In order for a Platform to be in harmony, it requires some lightweight **governance** to help communicate the needs of the products back to the self-serve platform, ensure that the Platform has a **clear goal** and its collaborators understand it, ensure that the **Platform continues to deliver value to the company**, and is a **lighthouse for good engineering and data practices**. This is a distributed group with representatives from multiple levels in multiple disciplinaries. The purpose of this team is set some lightweight guidelines to ensure the success of every participant in the platform and the success of the company, not to rule by command and control. This group is often comprised of forward-thinking and people- and business-understanding individuals from all areas of the platform.
# MAGIC
# MAGIC #### The Guiding Light: FAIR Principles
# MAGIC The most successful Platforms are guided by the [**FAIR Principles**](https://www.go-fair.org/fair-principles/) whose ultimate goal is to ensure **data sharing**. Hanging on to your data and keeping it a secret is out-of-date behaviour. The following text is directly from [**Go-FAIR**](https://www.go-fair.org/fair-principles/). The principles refer to three types of entities: **data** (or any digital object), **metadata** (information about that digital object), and **infrastructure**. For instance, principle F4 defines that both **metadata** and **data** are registered or **indexed** in a **searchable** resource (the infrastructure component).
# MAGIC
# MAGIC ##### Findable
# MAGIC The first step in (re)using data is to find them. Metadata and data should be **easy to find for both humans and computers**. Machine-readable metadata are essential for automatic discovery of datasets and services, so this is an essential component of the FAIRification process.
# MAGIC
# MAGIC * F1. (Meta)data are assigned a globally unique and persistent identifier
# MAGIC * F2. Data are described with rich metadata (defined by R1 below)
# MAGIC * F3. Metadata clearly and explicitly include the identifier of the data they describe 
# MAGIC * F4. (Meta)data are registered or indexed in a searchable resource
# MAGIC
# MAGIC ##### Accessible
# MAGIC Once the user finds the required data, she/he/they need to **know how they can be accessed,** possibly including **authentication** and **authorization**.
# MAGIC
# MAGIC * A1. (Meta)data are retrievable by their identifier using a standardised communications protocol
# MAGIC   * A1.1 The protocol is open, free, and universally implementable
# MAGIC   * A1.2 The protocol allows for an authentication and authorization procedure, where necessary
# MAGIC * A2. Metadata are accessible, even when the data are no longer available
# MAGIC
# MAGIC ##### Interoperable
# MAGIC The data usually need to be **integrated with other data**. In addition, the data need to interoperate with applications or workflows for **analysis**, **storage**, and **processing**.
# MAGIC
# MAGIC * I1. (Meta)data use a formal, accessible, shared, and broadly applicable language for knowledge representation
# MAGIC * I2. (Meta)data use vocabularies that follow FAIR principles
# MAGIC * I3. (Meta)data include qualified references to other (meta)data
# MAGIC
# MAGIC ##### Reusable
# MAGIC The ultimate goal of FAIR is to **optimize the reuse of data**. To achieve this, metadata and data should be **well-described** so that they can be replicated and/or combined in different settings.
# MAGIC
# MAGIC * R1. (Meta)data are richly described with a plurality of accurate and relevant attributes
# MAGIC   * R1.1. (Meta)data are released with a clear and accessible data usage license
# MAGIC   * R1.2. (Meta)data are associated with detailed provenance
# MAGIC   * R1.3. (Meta)data meet domain-relevant community standards

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Elements of a Data Product
# MAGIC Data Engineers are often part of a team that is building a **Data Product**. If we recall, a **_Data Product aims to answer a specific domain question_** to help a business make a decision or transform data according to a complex domain construct so that others can reuse the output shape of data. Data Products can be in the form of a simple data transformation through to a more complex ML model. They are cross-functional teams, very closely involve the Consumer or Decision-maker, and employ many Software Development sensible defaults like iterative development, deploying small changes, testing, and continuous deployment.
# MAGIC
# MAGIC
# MAGIC #### Identify your Consumer and what they want
# MAGIC If you think back in a business domain, there are some very specific questions that we could answer with data, but we don't usually talk about who was actually asking for that data. Some example personas could be:
# MAGIC
# MAGIC * A **Business Intelligence analyst** responsible for reporting to the business about the performance of a product against defined business metrics
# MAGIC * A **Product Owner** of an adjacent department or team looking to use specific parts of your data to generate business value through another product (ML-based product, a product to help make a business decision)
# MAGIC * A **Machine Learning Engineer** creating a recommendation engine using your data
# MAGIC
# MAGIC All of these personas have different requirements of how their data is **delivered** (e.g. APIs for highly aggregated small amount of data, emailable CSVs, a data stream, or an external location to the data) and how curated their data should be. A business analyst might want historical data at a high level of aggregation grouped by a time measure delivered to a visualisation tool like Power BI. A product owner might require a lightly-aggregated (near) real-time stream of data so that they can get real-time data that can be further transformed by the team for their use case. A machine learning specialist might require **a lot** of lightly-transformed historical signal data via a Blob Storage link which can be used to train a model.
# MAGIC
# MAGIC #### A Data Product Offering
# MAGIC The goal of a platform is to **accelerate the process of delivering value to a business** by consolidating certain efforts while still allowing a great amount of autonomy. One such offering is to share various levels of curated data in that domain such that the data is cleaned up (e.g., deduplicated), the fields/columns are understandable/well-documented, there is transparency (through documentation or Data Lineage tooling) about how the data was transformed, and the data is served to consumers according to their needs. This reduces the need for data wranglers in an organisation to solve the same problems over again (curation), and there is a consistent definition for whether or not a domain object is in a certain state (e.g., what features contribute to healthy/not-healthy). Additionally, not all people in an organisation, especially those far from tech, should need to understand the raw data and how to transform it in order to come to a conclusion for a business decision (but they should be aware that data is at the heart of a business decision). For example, not all sensor data is human-readable, and some might come in a binary format which must be decoded by a manufacturer-specific translation mapping, of which there are many. Could you imagine everyone in your organisation needing to do the same and sometimes tedious transformations for every product that used data? How would you ensure that all transformations happened consistently?
# MAGIC
# MAGIC **All data products should employ good product practices**, where all consumers/personas are heavily researched, it is clear what kind of data they need, how fresh that data needs to be, how accurate it needs to be, and what delivery mechanism is most desirable (defined by the consumers of your product).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Landmarks of Data Processing
# MAGIC Once the scope of a **Data Product** has been defined and it is clear what question is to be answered and the data requirements (accuracy, freshness, etc) have been defined, we can start to think about the steps in **Data Processing**. It is necessary to understand the scope and requirements because it will help us understand what types of tech and processing will be required. For example, if the data is required to be near-real time, we might choose to transform less (saves time) and use **streaming technologies**. If the data is required to be processed once per month, we might choose to **batch process** our data (saves money).
# MAGIC
# MAGIC Regardless of whether or we are doing _batch processing_ or _streaming_, there are a few landmarks which we can look out for in the world of data processing:
# MAGIC
# MAGIC * Data Sources
# MAGIC * Ingestion
# MAGIC * Transformation
# MAGIC * Intelligence
# MAGIC * Visualization
# MAGIC * Decisions and Actions
# MAGIC
# MAGIC #### Data Sources
# MAGIC This is the **source of data**. For many of our Data Engineering use cases, it is often **data that is generated** as a result of a transactional application (e.g. e-commerce application) and stored in a database that fits the transactional application's usecase. This could be a Postgres or MySql database, IoT devices, JSON/CSV files, or another data format as a result of previous processing.
# MAGIC
# MAGIC There's also plenty of open source datasets as well:
# MAGIC * [Our World in Data](https://github.com/owid)
# MAGIC * [NASA Data](https://data.nasa.gov/)
# MAGIC * [NYC Taxis](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
# MAGIC * [Open Movie Database](https://www.omdbapi.com/)
# MAGIC * [Reddit Corpus](https://github.com/webis-de/webis-tldr-17-corpus)
# MAGIC * [Physical Characteristics of Abalone](https://archive.ics.uci.edu/ml/datasets/abalone)
# MAGIC * [MNIST Database of Handwritten Digits](http://yann.lecun.com/exdb/mnist/)
# MAGIC * [Transcripts of all Friends (TV Show) episodes](https://www.kaggle.com/datasets/divyansh22/friends-tv-show-script)
# MAGIC
# MAGIC Some important questions to ask yourself when selecting a data source are:
# MAGIC * Does it come from a reputable source? 
# MAGIC * Was the data collected in a trustworthy and responsible fashion?
# MAGIC * Is the data biased? Does it omit key information?
# MAGIC * Is it too curated for what you need? (e.g. rounding, sampling)
# MAGIC * Do you understand how the data was aggregated and how it might affect your conclusions?
# MAGIC * Do you understand the domain and what each data point represents?
# MAGIC
# MAGIC #### Ingestion
# MAGIC This step is the **action of reading from the data source**. In **batch processing**, we often read or ingest data as-is or close to as-is so that it allows us to debug or reprocess data in case of pipeline/processing failures. In **streaming**, we read directly from a stream, like a Kafka Topic or AWS Kinesis Stream.
# MAGIC
# MAGIC #### Transformation
# MAGIC Once the data has been ingested, transformation logic **aggregates and shapes the data** into a form that can be easily underestood and analyzed, or used downstream in another pipeline. In batch processing, we sometimes see this in the form of Bronze, Silver, Gold stages (called "multi-hop" architecture) which represent increasingly curated levels of data. While they're a helpful guideline, it's not required to use the Bronze, Silver, and Gold multi-hop architecture strictly - sometimes it is enough to think through your problem and determine relevant and pragmatic stages from there.
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC ![bronze-silver-gold.png](./assets/bronze-silver-gold.png)
# MAGIC
# MAGIC Source: [Databricks](https://www.databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC In this space, we typically work with **data frames** (think: spreadsheets with columns and many rows that could be turned into a CSV file, but way cooler than spreadsheets 😎) and transform them using a SQL-like language and (if needed) distributed compute like Spark (for handling large amounts of data quickly). This part requires some **deeper knowledge of SQL**.
# MAGIC
# MAGIC #### Visualization
# MAGIC Typically, we spend quite some energy in the Ingestion/Transformation space to get the data into a specific shape but we mustn't forget about why we ingested/transformed data in the first place: to **give transparency and insights** into a system in order to make an informed decision or opinion. Visualizations are just one form of how we use data to bring value to a consumer and can come in the form of a graph/charts or list of data. Different forms of visualizations are deemed appropriate in efficiently communicating different kinds of messages.
# MAGIC
# MAGIC Some examples:
# MAGIC * [Line graph](https://en.wikipedia.org/wiki/Line_chart) to show linear trends
# MAGIC * [Scatter plot](https://en.wikipedia.org/wiki/Scatter_plot) to show the relationship between variables to determine a positive or negative correlation
# MAGIC * [Histogram](https://en.wikipedia.org/wiki/Histogram) to show the distribution of numerical data
# MAGIC * [Box (and whisker) plots](https://en.wikipedia.org/wiki/Box_plot) to show the spread and skewness of data
# MAGIC * [Log Chart](https://en.wikipedia.org/wiki/Logarithmic_scale), a non-linear method to display rapidly-growing values (e.g. exponential values) in a small graph
# MAGIC
# MAGIC Some types of charts like [bar plots](https://en.wikipedia.org/wiki/Bar_chart) can [hide or misrepresent the spread of data](https://www.kickstarter.com/projects/1474588473/barbarplots).
# MAGIC
# MAGIC #### Intelligence
# MAGIC Another consumer of curated data are **Intelligence applications**. This could be a **Machine Learning model** which uses analytical/curated data as training data and could drive a _recommendation engine_ in e-commerce or streaming media systems. Often, Machine Learning models require lots of (relevant) data for training and a different set of data for testing the model. Machine Learning Engineers work with Data Engineers to obtain the right kind of data for both of these stages. Sometimes a Data Scientist or Machine Learning Engineer will simply request \"all the raw data\" if the data is too well curated or aggregated but in reality, this is really just the start of a conversation and strong collaboration between a ML Engineer and a Data Engineer to leverage the best of the toolsets between the two expertises (e.g. distributed data processing engines, MLOps tooling). What's exciting about Intelligence applications is that they don't just inform a business decision but they also has the ability to feed back into Customer-facing applications (for example, recommendation engines).
# MAGIC
# MAGIC In this course, we won't build ML models, but you should be in a position to supply data to ML Engineers and understand how their needs influence the design of upstream data systems maintained by Data Engineers.
# MAGIC
# MAGIC #### Decisions and Actions
# MAGIC The end goal of any Data Product should be to **help a consumer / business to make an informed, data-led decision**. We called this an **Augmented Decisions** and that Decision should be **auditable** and **trackable**. Another type of Decision is a **\"Machine-Led Decision\"** which is an automated decision based on data input (for example, a change in status triggers a certain automated downstream behaviour). Sometimes, a valid output of a Data Product is a dashboard, but dashboards are often thrown over the wall and not looked at. If we cannot reliably inform a downstream decision or action, a Data Product does not have a purpose or value, which is why constant revalidation of models or other Data Product outputs is necessary.
# MAGIC
# MAGIC #### Intelligent Enterprises
# MAGIC If you're wanting to understand the various levels of maturity in an enterprise, check out the [Intelligent Enterprises Series](https://www.thoughtworks.com/insights/articles/intelligent-enterprise-series-models-enterprise-intelligence).
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hands-on
# MAGIC
# MAGIC Convert input data to an output product.
# MAGIC
# MAGIC **Input Format**: a log file `data/logs.jsonl` that it's a JSON per line [jsonl](https://jsonlines.org) as shown below:
# MAGIC </br></br>
# MAGIC ```
# MAGIC {"ua":"Mozilla/5.0 ArchLinux (X11; U; Linux x86_64; en-US) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.100 Safari/534.30","file":"UltricesPosuereCubilia.mp3","ip_address":"119.152.246.86","time":"2024-05-29T09:23:04Z","v":"4.8","status":500}
# MAGIC ```
# MAGIC
# MAGIC **Expected output format ist**: we want to have the 10 top most successfully downloaded files only

# COMMAND ----------

# Get the path of the current notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Strip away the last two segments (file name and current folder) to get the root folder of all learning modules
workspace_users_path = "/".join(notebook_path.split("/")[:-2])

print(f"Workspace Users Path: {workspace_users_path}")

# Prepare the environment
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
input_dir = f"/Workspace/{workspace_users_path}/Module 02 - Data Architecture/data"
output_dir = f"/Workspace/{workspace_users_path}/Module 02 - Data Architecture/output"

print("Input directory: ", input_dir)
print("output directory: ", output_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell, read all the log lines from the JSONL file (already done for you), create a DataFrame (already done for you), and build a small data product that displays the **top 10** most downloaded files.
# MAGIC Functions you might want to look up:
# MAGIC
# MAGIC Functions you meight want look up: 
# MAGIC - [groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
# MAGIC - [agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html?highlight=agg#pyspark.sql.DataFrame.agg)
# MAGIC - [count](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html?highlight=count#pyspark.sql.DataFrame.count)
# MAGIC - [alias](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html?highlight=alias)
# MAGIC - [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html?highlight=orderby)
# MAGIC - [limit](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html?highlight=limit)

# COMMAND ----------

import pandas as pd    
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Logs") \
    .getOrCreate()

# READ IN AS PANDAS, because we want to read from local file storage, not DBFS.
json_object = pd.read_json(path_or_buf=f"{input_dir}/logs.jsonl", lines=True)

# Now, transform the pandas dataframe to a spark dataframe
df = spark.createDataFrame(json_object)

### Put your code here.
#result = df

# Save the result into a parquet file
### Put your code here.

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                        |
# MAGIC |-------------------------------------------------------------------|
# MAGIC | <a href="$./3.2 Data Mesh" target="_self">Data Mesh</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>