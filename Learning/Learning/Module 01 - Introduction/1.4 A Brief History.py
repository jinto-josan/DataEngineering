# Databricks notebook source
# MAGIC %md
# MAGIC ## A Brief History
# MAGIC
# MAGIC It's important for you to know how the data ecosystem has evolved and which key technologies have paved the way to have what we have now. As Winston Churchill said, _“Those that fail to learn from history are doomed to repeat it.”_
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Stay informed about the pivotal technologies and landmark events that have shaped the evolution of the Data Engineering field to date.
# MAGIC * Understand the pros/cons of a Data Warehouse and Data Lake.
# MAGIC * Know about why Apache Hadoop failed its promises.
# MAGIC * Understand the separation of storage and compute and the query engines that emerged.
# MAGIC * Explain what a Data Lakehouse is and why it's relevant.
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## A little bit of History
# MAGIC
# MAGIC Watch the following [**video**](https://youtu.be/MxSwJQvWKuw) to get a summary of the history of key technologies and events that have influenced the Data Engineering Field as of today. Just keep in mind that some _patterns_, _technologies_, and _practices_ keep emerging.
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC **Story Timeline of Technologies and Events**
# MAGIC
# MAGIC |  Period | Technologies | Relevant Information to Know | 
# MAGIC |---| ---  | ---  |
# MAGIC | 1970 - 1999 |Databases, Data Warehousing, the Web| The birth of the data engineer arguably has its roots in **Data Warehousing**, dating as far back as the 1970s, with the business data warehouse taking shape in the 1980s and Bill Inmon officially coining the term **Data Warehouse** in **1989**. **Data Warehousing** and **Business Intelligence** engineering were a **precursor** to today’s data engineering and still play a central role in the discipline.|
# MAGIC |2000| Web Apps, MapReduce, Hadoop, AWS, S3  | Web companies continued to rely on the traditional monolithic, relational databases and data warehouses of the 1990s, pushing these systems to the limit, but a new generation of **cost-effective**, **scalable**, **available**, and **reliable** system was required. In **2004** Google published the [**_MapReduce whitepaper_**](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf), inspiring the **[Apache Hadoop](https://hadoop.apache.org/)** project to leverage commodity hardware that at that time was cheap. In addition, Amazon launched **AWS** and its first object storage solution called [**Amazon Simple Storage Service (S3)**](https://aws.amazon.com/s3/). The term **Big Data** was coined and the early big data tools and public cloud **laid the foundation** for today’s data ecosystem. The modern **data ecosystem** and **data engineering** as we know it now would not exist without these **innovations**.|
# MAGIC |2000 - 2010|Data Lakes, Hadoop Hype, Apache Spark, Presto| Companies started adopting **Hadoop** to create **Data Lakes** and it was a **big hype** in the industry. However it just lasted few years due to the complexity, learning curve, performance and not complaint with data regulations such as **[GDPR](https://gdpr.eu/what-is-gdpr/)**. Hadoop was victim of its own success and **[failed](https://www.datanami.com/2017/03/13/hadoop-failed-us-tech-experts-say/)** to commit its promises. In addition, Hadoop was highly-coupled and it was required to **separate computing from storage** to leverage the power of cloud computing and storage. For that reason, the **analytic query engines** such as [**Apache Spark**](https://spark.apache.org/) and [**Presto**](https://prestodb.io/) emerged.|
# MAGIC |2010 - Today| Data Lakehouse, Databricks, Delta Lake  |The need to take the best of Data Warehouses and Data Lakes led to [**Data Lakehouses**](https://www.databricks.com/product/data-lakehouse) where it's possible to get the best of both of them. Some data platforms such as **[Databricks](https://docs.databricks.com/en/introduction/index.html)** emerged and are paving the way for the next innovations in the field. Databricks's abstraction of a Data Lakehouse is called **[Delta Lake](https://delta.io/)**. Modern data platforms such as **Databricks**, **Snowflake**, **Microsoft Data Fabric** leverage the power of **distributed and cloud computing** by storing data in **object storage** solutions and **computing** it in clusters using analytic query engines.|
# MAGIC
# MAGIC **⚠️** **IMPORTANT:** The data ecosystem of tools, practices, platforms is still evolving and maturing pretty fast so it's important to be tuned of what will be next. Just review again **[The 2024 MAD (ML, AI & Data) Landscape](https://mad.firstmark.com/)**.
# MAGIC
# MAGIC 📚 **TO KNOW MORE ABOUT:**
# MAGIC
# MAGIC - [CAP Theorem Illustrated](https://www.youtube.com/watch?v=9SSvdLnmDiI)
# MAGIC - [Vertical Vs Horizontal Scaling: Key Differences You Should Know](https://www.youtube.com/watch?v=dvRFHG2-uYs)
# MAGIC - [Big Data Analytics](https://www.databricks.com/glossary/big-data-analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                | Next Topic                                                                  |
# MAGIC |---------------------------------------------------------------|-----------------------------------------------------------------------------|
# MAGIC | <a href="$./1.3 OLAP vs OLTP" target="_self">OLAP vs OLTP</a> | <a href="$./1.5 Set up your cluster" target="_self">Set up your cluster</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>