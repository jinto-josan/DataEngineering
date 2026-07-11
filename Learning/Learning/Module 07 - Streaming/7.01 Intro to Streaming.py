# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC What we have seen until now was only Spark SQL API which deals with batch use cases (scenarios where we deal with high volume, scheduled, high volume and repetitive jobs). Spark also has an API for streaming (i.e data in motion) use cases, called the Spark Structured Streaming API. Before we jump into the specifics of this API and see what it looks like, let us first understand why we do streaming, as in, what are the use cases which tells us that we have a streaming problem on hand.
# MAGIC
# MAGIC ## Streaming Use Cases
# MAGIC
# MAGIC Some of the key characteristics of streaming use cases would be when data under consideration is:
# MAGIC
# MAGIC - required to be delivered in real-time or near real-time
# MAGIC - unbounded and continuous
# MAGIC - ephemeral and not replayable
# MAGIC
# MAGIC There are many use cases across many domains where streaming applications are useful. The following are some of the scenarios for streaming use cases :
# MAGIC
# MAGIC - **Fraud Detection** - Industries across the globe collectively lose trillions of dollars in revenue annually due to fraud, and it is only getting worse as fraudsters become increasingly sophisticated. Stream processing frameworks here can minimize cases of fraud by processing and analysing real-time streams of records, recognising patterns, uncovering suspicious transactions and creating predictive and timely alerts for possible fraud.
# MAGIC
# MAGIC - **Log Analysis** - With log analysis, streaming frameworks can analyze the network logs in real time to detect anomalies and incidents before it affects the user. For e.g, log analysis of routers in real time can tell us about any suspicious activity and take preventive measures before it is too late.
# MAGIC
# MAGIC - **Online Advertising** - e-commerce and social network applications track user behaviour, clicks and interests in real time using streaming systems. These systems then promote personalized ad campaigns which the users might be interested in, in an expectation of increased revenues.
# MAGIC
# MAGIC Details of the above scenarios and a few others can be seen [here](https://datastorageasia.com/5-use-cases-of-stream-processing-that-demonstrate-its-business-value/)
# MAGIC
# MAGIC
# MAGIC ## Streaming Concepts
# MAGIC
# MAGIC Let us now shed some light on what streaming actually looks like. To start with, we will need to understand concepts like Data Streams, Streaming Systems and types of these systems.
# MAGIC
# MAGIC ### Data Stream
# MAGIC
# MAGIC > A data stream is the transmission of a sequence of digitally encoded signals to convey information. Typically, the transmitted symbols are grouped into a series of packets. -- [wiki](https://en.wikipedia.org/wiki/Data_stream)
# MAGIC
# MAGIC Such data streams are often created on a real-time or near real-time basis.
# MAGIC
# MAGIC ### Streaming System
# MAGIC
# MAGIC A Streaming system computes data directly as it is produced or received.
# MAGIC
# MAGIC ### Types of Streaming System
# MAGIC
# MAGIC _Stateless_ - processing stream of events without maintaining any state or knowledge of previous events.
# MAGIC - Example - Events from an IOT device where each event is individually processed
# MAGIC
# MAGIC _Stateful_ - processing stream of events and maintaining state or knowledge of previous events.
# MAGIC - Examples 
# MAGIC   - Number of users using the application at given point of time (This is super critical for OTT applications like netflix where password sharing is quite common and the application would want to restrict it)
# MAGIC   - The volumes of a particular product (an iphone, ipad etc.) sold in a particular time interval like an hour for an e-commerce application, requiring real-time analytics and aggregation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                |
# MAGIC |-------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./7.02 Spark Structured Streaming" target="_self">7.02 Spark Structured Streaming</a>| |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>