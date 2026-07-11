# Databricks notebook source
# MAGIC %md
# MAGIC ## Local Environment setup
# MAGIC
# MAGIC In this lesson you will be able to have your local environment set up. Despite the fact that you'll be using Databricks in your learning journey and you'll use notebooks; sometimes it's helpful to have your local machine ready to keep learning, practicing and coding in Python and Apache Spark.
# MAGIC
# MAGIC **⚠️** **IMPORTANT**: these instructions apply for MAC OS but might be tried in a Linux distribution too.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to
# MAGIC * Have installed Python, Java Development Kit (JDK), Apache Spark, and PyCharm CE IDE in your local environment
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites MAC OS
# MAGIC
# MAGIC Make sure you’ve installed **[homebrew](https://brew.sh/)** package manager
# MAGIC
# MAGIC You can use a different Shell such as **Zsh** ([Oh My Zsh!](https://ohmyz.sh/)) or others where bash profile referred in this guide will belong to the corresponding file (in default bash your bash profile is located on `**~/.bash_profile**` if you use Zsh it’s located on `**~/.zshrc**`)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before Starting
# MAGIC
# MAGIC For the local machine set up we’re going to use **Spark-3.0.2 with Hadoop2.7**, each **Apache Spark** distribution works with a **specific Python and Java version** so it’s important to review the compatibility matrix for the version that you’ll use and install the required versions for Java and Python:
# MAGIC
# MAGIC - [Python Compatibility Matrix for Spark Distribution](https://community.cloudera.com/t5/Community-Articles/Spark-Python-Supportability-Matrix/ta-p/379144)
# MAGIC - [Java Compatibility Matrix for Spark Distribution](https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669)
# MAGIC
# MAGIC For the version that we’ll work on we'll need **Python v3.9** and **JDK v1.8**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Python 3.9.6
# MAGIC 1. Open your terminal and execute 
# MAGIC > `brew install python@3.9.6`
# MAGIC
# MAGIC 2. In your terminal execute
# MAGIC > `python3` or `python` if you have set up that as an alias on your system 
# MAGIC <br/>
# MAGIC   You should have entered to the Python console
# MAGIC   <br><img src="assets/python.png">
# MAGIC   <br><p>To exit the REPL, simply type <code>quit()</code> and press the enter key.</p>
# MAGIC
# MAGIC 3. Close the Python console and install the **`Py4J`** package using **`pip`**
# MAGIC > `python -m pip install "py4j"`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing JDK Java Version 8
# MAGIC
# MAGIC 1. Download the JDK of Java Version 8 from the following [**link**](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html) 
# MAGIC <br/>**IMPORTANT NOTES:** **A)** You have to register with Oracle using an emailof your choice. In addition, if you are working with a **Mac with Apple M1 Pro Chipset** download the file `jdk-8u391-macosx-aarch64.dmg` else `jdk-8u391-macosx-x64.dmg`. **Note** If you get a `400` error while accessing the above link, please try clearing the website data, including cookies and try again. Also, if `jdk-8u391` is not avaiable at the above link or has reached the End of Life, please try downloading and using the latest `jdk-8xxxx` version. It should work alright for the setup below. **B)** If you don't want to register with Oracle, or have trouble logging in, you can try installing the JDK via `brew install --cask corretto@8` to install the Amazon Corretto version of the JDK.
# MAGIC 1. Execute the DMG (if you haven't used brew before) file to install the downloaded JDK
# MAGIC 1. If you need to work with different Java versions, install **[jenv](https://www.jenv.be/)**
# MAGIC - Open your terminal and execute 
# MAGIC > `brew install jenv`
# MAGIC
# MAGIC - Open your bash profile and add the following environment variables
# MAGIC > `export PATH="$HOME/.jenv/bin:$PATH"`<br>
# MAGIC > `eval "$(jenv init -)"`<br>
# MAGIC
# MAGIC - In your terminal, locate your installed Java Virtual Machines executing `sudo find / -name Java` (Most of the time you’ll find them on `/Library/Java/JavaVirtualMachines`)
# MAGIC
# MAGIC - Add the virtual machines to **jenv** executing the following command: 
# MAGIC > `jenv add /Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home`
# MAGIC
# MAGIC - List your virtual machines in **jenv** using the following command: 
# MAGIC > `jenv versions`
# MAGIC - Select the Java Version for the shell using the following command
# MAGIC > `jenv global <the_name_of_your_jvm>`
# MAGIC
# MAGIC - Don’t forget to source your bash profile
# MAGIC > `source .bash_profile (Default Bash)`<br>
# MAGIC > `source .zshrc (For Zsh)`
# MAGIC
# MAGIC 4. Finally execute `java -version` and make sure that you are using the `v1.8.0_xxx`<br/>
# MAGIC <br><img src="assets/jdk18.png"><br>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Apache Spark
# MAGIC 1. Download [**spark-3.0.2-bin-hadoop2.7.tgz**](https://archive.apache.org/dist/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz)
# MAGIC 1. Open your terminal, go the the folder where you downloaded file and decompress it using the following command:
# MAGIC > `tar -xvzf spark-3.0.2-bin-hadoop2.7.tgz`
# MAGIC >> **Attention**: If you downloaded the file via a browser, it may have been unzipped automatically.
# MAGIC >> In this case untar it only: `tar -xvf spark-3.0.2-bin-hadoop2.7.tar`
# MAGIC
# MAGIC 1. Run the command `pwd` and copy the full path
# MAGIC > `For example "/data-tools/spark-3.0.2-bin-hadoop2.7”`
# MAGIC 1. Open your bash profile and add the following environment variables
# MAGIC > `export SPARK_HOME="/data-tools/spark-3.0.2-bin-hadoop7"`<br>
# MAGIC > `export PYSPARK_PYTHON=python3.9.6`<br>
# MAGIC > `export PATH="$HOME/.jenv/bin:$PATH:$SPARK_HOME/bin"`<br>
# MAGIC 1. Source your bash profile
# MAGIC 1. Finally, in your terminal execute `pyspark` and you should be able to open the PySpark Console as shown below:
# MAGIC <br><br><img src="assets/pyspark.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing PyCharm CE
# MAGIC 1. Open your terminal and execute
# MAGIC > `brew install --cask pycharm-ce`
# MAGIC 1. Clone the following [Git repository](https://github.com/data-derp/exercise-vanilla-spark.git) that contains some exercises, make sure you are able to run "**00 - File Reads.py**" 
# MAGIC > **NOTE:** this code is for reference, feel free to review it, but it won't be used in the upcoming modules
# MAGIC 1. Configure **PyCharm** following these [instructions](https://github.com/data-derp/exercise-vanilla-spark/tree/main?tab=readme-ov-file#repo-set-up-in-pycharm)
# MAGIC 1. Restart **PyCharm** to have all set up
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up
# MAGIC
# MAGIC **Congratulations**, you have finished the Introductory module. As of now you should be clear of some **concepts** and **terminoloy** used in the Data Engineering field. In addition, you should be able to start working with **notebooks** and have your **cluster set up**. 
# MAGIC
# MAGIC **Excellent**, keep enjoying this learning journey!

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                        | Next Module                                                                                            |
# MAGIC |-----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./1.6 Notebooks basics" target="_self">Notebooks basics</a> | <a href="$../Module 02 - Data Architecture/2.0 Table of Contents" target="_self">Data Architecture</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>