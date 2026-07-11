# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Roles
# MAGIC
# MAGIC There are different roles that you will find in a Data Project or Product Team. It is quite important to know the differences and scope of tasks for these roles. Here are the objectives of this lesson.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Know about the most common roles in a data project or data product team.
# MAGIC * Explain the main purpose of a Data Engineer in your own words.
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 10 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Scientist vs. Data Engineer vs. Data Analyst
# MAGIC
# MAGIC Watch the following [video](https://youtu.be/y3TxcejHw-4) to get an overwiew of the three common roles that you'll find: **Data Engineer**, **Data Scientist** and **Data Analyst**.
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC - **Core purpose of a data engineer:** is designing robust and reliable systems to prepare data and serve it according to the needs of end users (_Data Analysts_, _Data Scientists_, _Machine Learning Engineers_, _Business Users_ and so on). For this reason, sometimes a Data Engineer is compared to a **plumber** dealing with all the data **pipelines** and making sure that **data flows** with **quality** to its destination.
# MAGIC
# MAGIC - **Data Scientist:** are professionals with a strong foundation in **mathematics**, **statistics**, and **computer science**. Their primary role is to **build models** that analyze data to **make predictions** and **provide actionable recommendations** for the **future**. While data scientists perform some exploratory work to understand data initially, their focus should not be on spending the majority (70–80%) of their time collecting, cleaning, and preparing data. This is where **data engineers play a crucial role**. Data engineers design and build systems to automate and scale the data preparation process, ensuring that data scientists have clean, reliable, and accessible data to work with. By handling the technical infrastructure and automating repetitive tasks, **data engineers enable data scientists** to focus on advanced analysis and model development, maximizing their productivity and impact.
# MAGIC
# MAGIC - **Data Analyst:** seeks to understand **business performance and trends**. Whereas data scientists are forward-looking, a data analyst typically focuses on the **past or present**. They use tools such as _Tableau_, _Looker Studio_, _Power BI._ They usually run **_SQL_** queries in the Data Warehouse or Data Lake. A data analyst’s typical downstream customers are _business users_, _management_, and _executives_. Data Analysts support data engineers in improving **data quality**.
# MAGIC
# MAGIC - **Machine learning (ML) engineers and AI researchers:** ML engineers overlap with data engineers and data scientists. ML engineers develop advanced ML techniques, train models, and design and maintain the infrastructure running ML processes in a scaled production environment. They know about ML and deep learning and use frameworks such as [_PyTorch_](https://pytorch.org/) or [_Tensorflow_](https://www.tensorflow.org/) and techniques such as [_MLOPs_](https://docs.databricks.com/en/machine-learning/mlops/mlops-workflow.html). 
# MAGIC
# MAGIC **⚠️** **CALL OUT**: the boundaries between ML engineering, data engineering, and data science are still blurry, so don't be surprised if you find mismatches and misconceptions of these roles in your client's place.

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                        | Next Topic                                                    |
# MAGIC |-----------------------------------------------------------------------|---------------------------------------------------------------|
# MAGIC | <a href="$./1.1 Data Engineering" target="_self">Data Engineering</a> | <a href="$./1.3 OLAP vs OLTP" target="_self">OLAP vs OLTP</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>