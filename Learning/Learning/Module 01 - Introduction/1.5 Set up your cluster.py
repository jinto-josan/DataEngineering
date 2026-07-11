# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up your cluster
# MAGIC
# MAGIC A Databricks cluster is a set of computation resources and configurations on which you run **data engineering**, **data science**, and **data analytics workloads**, such as production **ETL pipelines**, **streaming analytics**, **ad-hoc analytics**, and **machine learning**. You run these **workloads** as a set of **commands** in a **notebook** or as an **automated job**. 
# MAGIC
# MAGIC
# MAGIC Databricks makes a distinction between all-purpose clusters and job clusters.
# MAGIC
# MAGIC - You use **all-purpose clusters** to analyze data collaboratively using interactive **notebooks**.
# MAGIC - You use **job clusters** to run fast and robust **automated** jobs.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use the Clusters UI to configure and deploy a cluster
# MAGIC * Edit, terminate, restart, and delete clusters
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 15 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Cluster
# MAGIC
# MAGIC Depending on the workspace in which you're currently working, you may or may not have cluster creation privileges. 
# MAGIC
# MAGIC Instructions in this section assume that you **do have cluster creation privileges**, and that you need to deploy a new cluster to execute the lessons in this course.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Use the left sidebar to navigate to the **Compute** page
# MAGIC 1. Click the blue **Create Compute** button
# MAGIC 1. For the **Cluster name**, use your name so that you can find it easily
# MAGIC 1. Make sure that **Min Cluster Policy** is selected
# MAGIC 1. Set the **Access Mode** to **Single user** [This can now be seen in the `Advanced` section at the bottom, also changed as `Dedicated`]
# MAGIC 1. Set the **Databricks runtime version** to **17.3 LTS**
# MAGIC 1. In the **Single User Access** ensure that your username is selected
# MAGIC 1. Click on the **Create compute** button
# MAGIC
# MAGIC <br/><img src="assets/cluster_creation.png">
# MAGIC
# MAGIC **NOTE:** Clusters can take several minutes to deploy. Once you have finished deploying a cluster, feel free to continue to explore the cluster creation UI.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Clusters
# MAGIC
# MAGIC Once the cluster is created, go back to the **Compute** page to view the cluster.
# MAGIC
# MAGIC Select **your cluster** to review the current configuration. 
# MAGIC
# MAGIC Click the **Edit** button. Changing most settings will require running clusters to be restarted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restart, Terminate, and Delete
# MAGIC
# MAGIC Note that while **Restart**, **Terminate**, and **Delete** have different effects, they all start with a cluster termination event. (Clusters will also terminate automatically due to inactivity based in the termination policy created for this course - 30 mins of inactivity)
# MAGIC
# MAGIC When a cluster terminates, all cloud resources currently in use are deleted. This means:
# MAGIC
# MAGIC * Associated VMs and operational memory will be purged
# MAGIC * Attached volume storage will be deleted
# MAGIC * Network connections between nodes will be removed
# MAGIC
# MAGIC In short, all resources previously associated with the compute environment will be completely removed. This means that **any results that need to be persisted should be saved to a permanent location**. Note that you will not lose your code, nor will you lose data files that you've saved out appropriately.
# MAGIC
# MAGIC The **Restart** button will allow us to manually restart our cluster. This can be useful if we need to completely clear out the cache on the cluster or wish to completely reset our compute environment.
# MAGIC
# MAGIC The **Terminate** button allows us to stop our cluster. We maintain our cluster configuration setting, and can use the **Restart** button to deploy a new set of cloud resources using the same configuration.
# MAGIC
# MAGIC The **Delete** button will stop our cluster and remove the cluster configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credits
# MAGIC
# MAGIC This content has been taken from the Databricks Official Course: [**Data Engineering with Databricks**](https://partner-academy.databricks.com/learn/course/1266/data-engineering-with-databricks?generated_by=623943&hash=6baf28bef13e38cb6146351602f1fba2c6d003cf).

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                      | Next Topic                                                            |
# MAGIC |---------------------------------------------------------------------|-----------------------------------------------------------------------|
# MAGIC | <a href="$./1.4 A Brief History" target="_self">A Brief History</a> | <a href="$./1.6 Notebooks basics" target="_self">Notebooks basics</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>