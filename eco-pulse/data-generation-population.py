# Databricks notebook source
# DBTITLE 1,Population Header
# MAGIC %md
# MAGIC **Indian Population Data**

# COMMAND ----------

# DBTITLE 1,Install kagglehub
!pip install kagglehub

# COMMAND ----------

# DBTITLE 1,Download population dataset
import kagglehub
import pandas as pd

# Download dataset
path = kagglehub.dataset_download("rdatta871/population-of-india")
print("Path to dataset files:", path)

# See what files are available
import os
print(os.listdir(path))

# COMMAND ----------

# DBTITLE 1,Read and inspect data
# Replace 'population_india.csv' with the actual filename from os.listdir above
df = pd.read_csv(f"{path}/population_india.csv")

print(df.shape)
print(df.head())
print(df.columns.tolist())
