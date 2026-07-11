# Databricks notebook source
# MAGIC %md
# MAGIC ## Amazon Kinesis
# MAGIC
# MAGIC Amazon Kinesis is AWS's competitive offering to kafka. Further below, we have summarized the capabilites of Amazon Kinesis for your reference:
# MAGIC
# MAGIC Here is a [short video introduction](https://youtu.be/07iZOEl0knc?si=3j3g8a6GJ-Agwpw-) to it and [an article](https://aws.amazon.com/kinesis/) from AWS with all the details.
# MAGIC
# MAGIC ![](./assets/aws-kinesis-offerings.png)
# MAGIC
# MAGIC So there are 4 main categories of Amazon Kinesis Capabilities as follows:
# MAGIC
# MAGIC - Kinesis Video Streams
# MAGIC - Kinesis Data Stream
# MAGIC - Kinesis Data Firehose
# MAGIC - Kinesis Data Analytics
# MAGIC
# MAGIC Lets look at each of them in a little more detail below:
# MAGIC
# MAGIC ### Kinesis Data Stream
# MAGIC
# MAGIC Kinesis Data Stream is an alternative to Kafka. It is essentially a message broker cluster that you can run your streaming and messaging services on.
# MAGIC
# MAGIC ![](./assets/kinesis-data-stream.png)
# MAGIC
# MAGIC
# MAGIC Here is a [you tube video](https://youtu.be/hLLgkTUmwOU?si=yOcA0X93FjAwTLVi) on data stream and [an article](https://aws.amazon.com/kinesis/data-streams/) from AWS with all the details for your reference.
# MAGIC
# MAGIC ### Kinesis Data Firehose
# MAGIC
# MAGIC The main purpose of firehose is basically to connect to your Kinesis Cluster, capture the data, and load it into durable storage service (for example, S3 or a database).
# MAGIC
# MAGIC ![](./assets/kinesis-data-firehose.png)
# MAGIC
# MAGIC Here is an [an article](https://aws.amazon.com/firehose/) from AWS with all the details for your reference.
# MAGIC
# MAGIC ### Kinesis Data Analytics
# MAGIC
# MAGIC Up to now, with Kinesis Data Stream and Kinesis Data Firehose, we have pipes sending data from one point to another. Kinesis Data Analytics helps develop applications that contain logic to either query or transform your data when it arrives through Kinesis Data Stream or Kinesis Data Firehose.
# MAGIC
# MAGIC
# MAGIC ![](./assets/kinesis-data-analytics.png)
# MAGIC
# MAGIC Here is an [an article](https://aws.amazon.com/managed-service-apache-flink/) from AWS with all the details for your reference. [Another video ](https://youtu.be/SX_6x_wXIfA?si=kk63AqWqiDvJ3RRB) detailing the use Data Analytics Studio.
# MAGIC
# MAGIC You can use Kinesis Data Analytics in conjunction with the rest of the Kinesis ecosystem (and AWS would probably recommend that). But depending on your use case, you could technically use AWS Glue or Databricks to read from Kinesis and use the Spark Streaming API, whereas with Kinesis Data Analytics, it only supports Apache Flink and Beam using Flink Runners (Spark Streaming won't exist in the Kinesis service). If you really like Spark Streaming, it might be recommendable to pick another service that can connect to data streams.
# MAGIC
# MAGIC There are benefits to using Kinesis Data Analytics (and Apache Flink), It’s designed to address continuous processing of data and it's a specialised framework to deal with streaming analytics. With Kinesis Data Analytics, you won't have to set up your own Flink cluster and all of the infrastructure for distributed systems; you can basically specify the amount of throughput units or the amount of data. The workflow might simply be to: specify the capacity that you want for Kinesis Data Analytics, submit your Flink logic as a jar, and it’ll run it for you.
# MAGIC
# MAGIC ### Reflection
# MAGIC
# MAGIC **Question:** Why might you want to run a service such as Kinesis Data Analytics over building your own Kubernetes Cluster with Flink?
# MAGIC
# MAGIC **Answer:** Maintenance overhead. It’s expensive and it could be painful to upgrade the version of Flink. Cloud offerings or other platforms might be more easily maintainable. An example of a platforms might be Ververica, which was developed by the creators of Flink. Simply create a job and you can submit, stop, or cancel it. Additionally, Kinesis handles the persistence of state - anything that needs some advanced data wrangling and advanced data processing will often need state. State needs to be managed very carefully (it can get really big and perhaps Kinesis will be a bit smarter than you when it comes to managing that state and making sure that your cluster didn’t blow up). Anything that is not bespoke to your application and business logic, try to offload that to infrastructure vendors or cloud services.
# MAGIC
# MAGIC
# MAGIC Finally, [a video](https://youtu.be/uWUAcc68MWI?si=BskkyevwHR2ofYY5) which puts it all together to complete your learnings of cloud offerings from AWS.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                                            |
# MAGIC |-----------------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./9.7 Cloud Offerings - Azure" target="_self">9.7 Cloud Offerings - Azure</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>