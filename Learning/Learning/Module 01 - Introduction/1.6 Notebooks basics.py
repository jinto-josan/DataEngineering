# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebooks Basics
# MAGIC
# MAGIC Notebooks are the primary means of developing and executing code interactively on Databricks. This lesson provides a basic introduction to working with Databricks notebooks.
# MAGIC
# MAGIC If you've previously used notebooks but this is your first time executing a notebook in Databricks, you'll notice that basic functionality is the same.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Attach a notebook to a cluster
# MAGIC * Execute a cell in a notebook
# MAGIC * Set the language for a notebook
# MAGIC * Describe and use magic commands
# MAGIC * Create and run a SQL cell
# MAGIC * Create and run a Python cell
# MAGIC * Create a markdown cell
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 15 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach to a Cluster
# MAGIC
# MAGIC In the previous lesson, you should have either deployed a cluster or identified a cluster that an admin has configured for you to use.
# MAGIC
# MAGIC At the top right corner of your screen, click the cluster selector (**"Connect" button**) and choose the cluster that you created from the dropdown menu. When the notebook is connected to a cluster, this button shows the name of the cluster.
# MAGIC
# MAGIC **NOTE**: Deploying a cluster can take several minutes. A **solid green circle** will appear to the left of the cluster name once resources have been deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks Basics
# MAGIC
# MAGIC Notebooks provide **cell-by-cell execution of code**. Multiple languages can be mixed in a notebook. Users can add plots, images, and markdown text to enhance their code.
# MAGIC
# MAGIC Throughout this course, our notebooks are designed as **learning instruments**. Notebooks can be easily deployed as production code with Databricks, as well as providing a robust toolset for **data exploration**, **reporting**, and **dashboarding**.
# MAGIC
# MAGIC ### Running a Cell
# MAGIC Run the cell below using one of the following options:
# MAGIC
# MAGIC * **CTRL+ENTER** or **CTRL+RETURN**
# MAGIC * **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
# MAGIC * Using **Run Cell**, **Run Selected Text**, **Run All Above** or **Run All Below** as seen here <br/><img src="assets/execute-notebook.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Give it a try if you can by attaching this notebook to the cluster created in the previous section and running the cell below.

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting the Default Notebook Language
# MAGIC
# MAGIC The cell above executes a Python command, because our current default language for the notebook is set to Python.
# MAGIC
# MAGIC Databricks notebooks support **Python**, **SQL**, **Scala**, and **R**. A language can be selected when a notebook is created, but this can be changed at any time.
# MAGIC
# MAGIC The default language appears to the right of the notebook title at the top of the page. Throughout this course, we'll use a blend of SQL and Python notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create and Run a SQL Cell
# MAGIC
# MAGIC * Highlight this cell and press the **B** button on the keyboard to create a new cell below
# MAGIC * Copy the following code into the cell below and then run the cell
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **NOTE**: There are a number of different methods for adding, moving, and deleting cells including GUI options and keyboard shortcuts. Refer to the <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">docs</a> for details.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Markdown
# MAGIC
# MAGIC The magic command <strong><code>&#37;md</code></strong> allows us to render Markdown in a cell:
# MAGIC * Double click this cell to begin editing it
# MAGIC * Then hit **`Esc`** to stop editing
# MAGIC
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC
# MAGIC This is only a test.
# MAGIC
# MAGIC This is text with a **bold** word in it.
# MAGIC
# MAGIC This is text with an *italicized* word in it.
# MAGIC
# MAGIC This is an ordered list
# MAGIC 1. one
# MAGIC 1. two
# MAGIC 1. three
# MAGIC
# MAGIC This is an unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC Images:
# MAGIC ![Databricks](https://cdn.bfldr.com/9AYANS2F/at/k8bgnnxhb4bggjk88r4x9snf/databricks-symbol-color.svg?auto=webp&format=png&width=150&height=166)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Juan   | 1     |
# MAGIC | João   | 2     |
# MAGIC | John   | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Magic Commands
# MAGIC Magic commands are specific to the Databricks notebooks. They are very similar to magic commands found in comparable notebook products. These are built-in commands that provide the same outcome regardless of the notebook's language
# MAGIC
# MAGIC A **single percent (%)** symbol at the start of a cell identifies a magic command, keep in mind that:
# MAGIC   * You can only have one magic command per cell
# MAGIC   * A magic command must be the first thing in a cell for that reason you should have seen: %md, %python, %sql until now

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Language Magics
# MAGIC Language magic commands allow for the execution of code in languages other than the notebook's default. In this course, we'll see the following language magics:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC Adding the language magic for the currently set notebook type is not necessary.
# MAGIC
# MAGIC When we changed the notebook language from Python to SQL above, existing cells written in Python had the <strong><code>&#37;python</code></strong> command added.
# MAGIC
# MAGIC **NOTE**: Rather than changing the default language of a notebook constantly, you should stick with a primary language as the default and only use language magics as necessary to execute code in another language.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credits
# MAGIC
# MAGIC This content has been taken from the Databricks Official Course: [**Data Engineering with Databricks**](https://partner-academy.databricks.com/learn/course/1266/data-engineering-with-databricks?generated_by=623943&hash=6baf28bef13e38cb6146351602f1fba2c6d003cf).

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                              | Next Topic                                                                                    |
# MAGIC |-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./1.5 Set up your cluster" target="_self">Set up your cluster</a> | <a href="$./1.7 Local Environment setup" target="_self">Local Environment setup</a> |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>