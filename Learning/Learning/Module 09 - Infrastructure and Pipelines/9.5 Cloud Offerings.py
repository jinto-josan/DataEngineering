# Databricks notebook source
# MAGIC %md
# MAGIC ## Cloud Offerings
# MAGIC
# MAGIC While Kafka can be run on-prem, it can be highly recommended to use cloud offerings for both distributed logs or Kafka itself. There are two very popular offerings, depending on which cloud provider you're using. While they functionally do the same things, **Kinesis** is an alternative to Kafka: it's not interoperable but uses the same concepts that Kafka has but with slightly different namings. For example, `partitioning` in Kafka is called `sharding` on Kinesis. Amazon has very recently come up with **Amazon MSK** (managed streaming for Apache Kafka). At the time of writing, with AWS customers, Kinesis is still much more popular service.
# MAGIC
# MAGIC **Azure Event Hubs**, like Kinesis, it has all the same bells and whistles. It scales automatically, which you might struggle with if you’re deploying your own Kafka cluster (one key reason to go for a cloud offering) on-prem. Unlike Kinesis, the resources that you create are actually fully compatible with Kafka’s APIs. If you have a streaming framework or application that you need to send messages to some kind of messaging bus, you can use the Kafka SDKs to send those messages. For example, if you’re using Spark Streaming or Apache Flink to write these applications, you can pretend that you’re writing your data out or reading your data from Kafka endpoints, unlike Kinesis. With Kinesis, you rely on the Streaming Framework to have a connector to Kinesis, but with Event Hub, anything that has a connector to Kafka will work.
# MAGIC
# MAGIC In the next sections, we'll cover Cloud Offerings from both AWS and Azure.

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                                            |
# MAGIC |-----------------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./9.6 Cloud Offerings - AWS" target="_self">9.6 Cloud Offerings - AWS</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>