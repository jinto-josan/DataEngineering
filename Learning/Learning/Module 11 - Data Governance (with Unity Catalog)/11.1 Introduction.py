# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC Welcome to the Data Governance (with Unity Catalog) lesson. In this lesson we will help you learn the basics of Data Governance with the help of Unity Catalog feature of databricks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to explain:
# MAGIC * what is Data Governance.
# MAGIC * what is Databricks `Data Engineer Learning Plan`
# MAGIC * what is Unity Catalog in Databricks and how it is related to governance
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 120 minutes
# MAGIC
# MAGIC ## Data Governance
# MAGIC
# MAGIC Data governance is a set of policies, processes, and technologies that ensure an organization's data is accurate, secure, and usable throughout its life cycle. It's a crucial part of any organization that uses data to drive business growth and make decisions.
# MAGIC
# MAGIC Data governance involves:
# MAGIC
# MAGIC * **Understanding data:** Understanding the origin, sensitivity, and lifecycle of all data. 
# MAGIC * **Data classification:** Organizing and categorizing data based on its value, sensitivity, and criticality 
# MAGIC * **Data quality:** Ensuring data is accurate and high quality 
# MAGIC * **Data protection:** Protecting data from security breaches and regulatory risks 
# MAGIC * **Data management:** Establishing processes for managing data
# MAGIC
# MAGIC You can read more in detail about it at this [link.](https://www.spiceworks.com/tech/big-data/articles/what-is-data-governance-definition-importance-and-best-practices/)
# MAGIC
# MAGIC ## Data Engineer Learning Path
# MAGIC
# MAGIC Databricks [`Data Engineer Learning Plan` ](https://partner-academy.databricks.com/learn/lp/10/data-engineer-learning-plan)is one of the many learning plans available on the Databricks Partner Portal. It comprises of many self paced and instructor led trainings leading to certifications like `Databricks Fundamental Accreditation`, `Databricks Data Engineer Associate` and `Databricks Data Engineer Professional`. You might have already gone through one such training and certification on `Module 10` of this course. The reason why we are talking about `Data Engineer Learning Plan` here is as follows :
# MAGIC
# MAGIC * Introduce you to the learning plan itself and its associated certifications. 
# MAGIC * Introduce you to `Data Engineering with Databricks` training which leads to `Databricks Data Engineer Associate` certification . This training now comprises of 4 self paced courses : [Data Ingestion with Lakeflow Connect](https://partner-academy.databricks.com/learn/courses/2963/data-ingestion-with-lakeflow-connect?hash=c9b659e878f2a65f9198ad74b1dcb9684239ecc7&generated_by=281371), [Deploy Workloads with Lakeflow Jobs](https://partner-academy.databricks.com/learn/courses/2979/deploy-workloads-with-lakeflow-jobs?hash=21b2147e32d28c3cb4753b3edea38cc23670953c&generated_by=281371), [Build Data Pipelines with Lakeflow Declarative Pipelines](https://partner-academy.databricks.com/learn/courses/2970/build-data-pipelines-with-lakeflow-declarative-pipelines?hash=e5fc4abefd180666d7f093b5257c764651afb833&generated_by=281371) and [Data Management and Governance with Unity Catalog](https://partner-academy.databricks.com/learn/courses/3144/data-management-and-governance-with-unity-catalog?hash=d5a5d0448aa8a64f6e231c73a3ca6919f6872f27&generated_by=281371)
# MAGIC * Guiding you to complete the `Data Management and Governance with Unity Catalog` course to understand more about Data Governance using Unity Catalog.[Details in the next section]. 
# MAGIC
# MAGIC ## Unity Catalog - Concept and Course Details
# MAGIC
# MAGIC Unity Catalog is a governance solution for data and AI assets in the Databricks Data Intelligence Platform. It provides a centralized place to manage data access, audit, and discover data.
# MAGIC
# MAGIC Here is link to [`Data Management and Governance with Unity Catalog`](https://partner-academy.databricks.com/learn/courses/3144/data-management-and-governance-with-unity-catalog?hash=d5a5d0448aa8a64f6e231c73a3ca6919f6872f27&generated_by=281371) course to build some solid fundamental concepts in Databricks Unity Catalog. Do log in to the databricks partner portal along the way if required.
# MAGIC
# MAGIC Its a 2 hour long self paced course with a small test at the end of the course. Completing the entire course is not an expectation but highly advisable if you would like to build the fundamenal concepts of Unity Catalog in Databricks.
# MAGIC
# MAGIC Lastly, the above course is largely theoritical. Most of the concepts explained here are used in the handson exercise in the next section.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                         |
# MAGIC |----------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./11.2 Create and Govern Data with UC" target="_self">Create and Govern Data with UC</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>