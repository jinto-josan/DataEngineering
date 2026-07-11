# Databricks notebook source
# MAGIC %md
# MAGIC # Visualisation & Accessibility
# MAGIC
# MAGIC In this lesson you will learn key concepts of **data visualization** and its role in making data-driven decisions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Differentiate **Exploratory** vs. **Explanatory** Analysis
# MAGIC * Choose the **Right Visuals**
# MAGIC * Reduce cognitive load and eliminate clutter for your audience
# MAGIC
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 30 minutes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Overview
# MAGIC Once data has been shaped according to a consumer's needs, they'll likely want to **drive insights, decisions, and actions** using that data. Sometimes this data is pushed into dedicated storage for a specific tool (e.g. [_PowerBI_](https://www.microsoft.com/en-us/power-platform/products/power-bi)) and other times, consumers will opt to read directly from a data source that you provide (direct federated access or through a federated API). Regardless of the method, it generally isn't impactful to simply show a table of data to someone and say "_that's our data, go make a decision_"; most decision-makers want the data **summarised**, **interpreted**, and **represented** in a way that it is easy to derive insights that can drive a decision to stay on course or to make a change.
# MAGIC
# MAGIC In this section, we'll first talk about how to represent data in **dashboards**, because it is typically the most common element in **Business Intelligence**. We'll also offer a list of references for Visualisation tooling for you to browse. Additionally, we'll do a practical exercise to create a visualisation based on a previous exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC # Understanding Your Data
# MAGIC
# MAGIC ## Using a Critical Eye
# MAGIC
# MAGIC Visualization helps bring out the story of your data and being able to tell these stories with data is of great significance for data‐driven decision making.
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"90%",width:"90%"}}>
# MAGIC
# MAGIC ![ticket_example.png](./assets/ticket_example.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - How does the volume of received and processed tickets vary per month?
# MAGIC - Are we falling behind in terms of processing the received tickets? If so, from when and why?
# MAGIC - What are the factors slowing down the ticket processing? Are these factors correlated? [Correlation is NOT causation if you are interested to learn more](https://www.youtube.com/watch?v=ROpbdO-gRUo)
# MAGIC
# MAGIC **Exploratory vs Explanatory Analysis**
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"50%",width:"50%"}}>
# MAGIC
# MAGIC ![explanatory_vs_exploratory.png](./assets/explanatory_vs_exploratory.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - **Exploratory** analysis is **what you do to understand the data**, finding patterns, outliers, relationships and so on.
# MAGIC - **Explanatory** analysis is **communicating the key insights** of the analysis to decision-makers, stakeholders, etc.
# MAGIC
# MAGIC ## Choose an effective visual
# MAGIC
# MAGIC ### **Simple Text**
# MAGIC
# MAGIC Simple text is used for communicating for numbers by making the numbers as prominent as possible and a few supporting words to clearly make your point.
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"25%",width:"25%"}}>
# MAGIC
# MAGIC ![simple_text.png](./assets/simple_text.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ### **Tables**
# MAGIC
# MAGIC Tables can be used for communicating to a mixed audience whose members will each look for their particular row of interest.
# MAGIC
# MAGIC - Different units of measure can be elegantly displayed on tables
# MAGIC - Allow the data to take a center stage and lighten the borders
# MAGIC - Heat maps can be used to provide visual cues so that potential points of interest can be easily spotted
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"55%",width:"55%"}}>
# MAGIC
# MAGIC ![heatmap_vs_table.png](./assets/heatmap_vs_table.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ### **Graphs**
# MAGIC
# MAGIC A well-designed graph is more effective than a table as it interacts with our visual processing system.
# MAGIC
# MAGIC **1. Points**
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"45%",width:"45%"}}>
# MAGIC
# MAGIC ![scatterplot.png](./assets/scatterplot.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - Scatterplots are useful for showing the relationship between two entities
# MAGIC - They encode data simultaneously on a horizontal x‐axis and vertical y‐axis and allow people to see what relationship exists.
# MAGIC - They are more frequently used in scientific fields than in the business world.
# MAGIC
# MAGIC **2. Lines**
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"45%",width:"45%"}}>
# MAGIC
# MAGIC ![line_plot.png](./assets/line_plot.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - Line graphs are most commonly used to plot continuous data, which is often in the form of some unit of time: days, months, quarters, or years
# MAGIC - They may not make sense for categorical data as the points in the graph are physically connected via a line
# MAGIC
# MAGIC **3. Bars**
# MAGIC
# MAGIC - Bar charts are allow for easy processing of visual information as our eyes compare the end points of the bars to find the largest, smallest and incremental difference
# MAGIC - Bar charts must always have a zero baseline
# MAGIC - In general the bars should be wider than the white space between the bars
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"60%",width:"60%"}}>
# MAGIC
# MAGIC ![bar_plot_width.png](./assets/bar_plot_width.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - Beware of stacked bar plots as they can overwhelm your audience with information
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"60%",width:"60%"}}>
# MAGIC
# MAGIC ![stacked_bar_plot.png](./assets/stacked_bar_plot.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - Horizontal bar charts are extremely useful for categorical data with long category names
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"70%",width:"70%"}}>
# MAGIC
# MAGIC ![horizontal_bar_plot.png](./assets/horizontal_bar_plot.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ### **Caveats**
# MAGIC
# MAGIC - Never use 3D charts unless it is absolutely necessary to add a third dimension. 3D charts introduce skews, making the data difficult to interpret and compare
# MAGIC - Pie charts should be mostly avoided when representing quantitative information as it becomes almost impossible to discern segments close in size. (Use bar charts instead)
# MAGIC - Avoid using secondary axes as it makes the interpretation of the data tedious
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"70%",width:"70%"}}>
# MAGIC
# MAGIC ![secondary_axes.png](./assets/secondary_axes.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ## Eliminate clutter
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"50%",width:"50%"}}>
# MAGIC
# MAGIC ![you_see_with_your_brain.png](./assets/you_see_with_your_brain.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC - A human brain has a finite amount of mental processing power to process visual information (or cognitive load)
# MAGIC - Cognitive load is the mental effort that is required to learn new information
# MAGIC - Clutter in our visualization results in extraneous cognitive load and processing that takes up mental resources but doesn’t necessarily improve one's understanding of the data
# MAGIC - Clutter has to be avoided at all costs as it simply eats up space and make the visualization feel more complicated
# MAGIC - [Gestalt Principles of Visual Perception](https://excelcharts.com/data-visualization-excel-users/gestalt-laws/) can help distinguish between clutter and useful information
# MAGIC
# MAGIC <div style={{textAlign:"center",height:"75%",width:"75%"}}>
# MAGIC
# MAGIC ![visual_order.png](./assets/visual_order.png)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ### Additional References
# MAGIC * [Storytelling with data](https://www.storytellingwithdata.com/books)

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualization Tools
# MAGIC The following is a compilation of several popular plotting and dashboarding libraries/tools (for reference only). It's most important to be familiar with the names. Come back to this when you need it! 
# MAGIC
# MAGIC ## Plotting and Visualization Libraries
# MAGIC
# MAGIC * [**Matplotlib**](https://matplotlib.org/): is a visualization library in Python
# MAGIC * [**Seaborn**](https://seaborn.pydata.org/): statistical data visualization in Python built upon matplotlib
# MAGIC * [**Plotly**](https://plotly.com/graphing-libraries/): build beautiful interactive visualizations without needing lots and lots of Javascript
# MAGIC * [**Plotly Express**](https://plotly.com/python/plotly-express/): build beautiful interactive visualizations in literally a few lines
# MAGIC
# MAGIC ## Analytics and Visualization Dashboards
# MAGIC
# MAGIC * [**Tableau**](https://www.tableau.com/)
# MAGIC * [**Microsoft Power BI**](https://powerbi.microsoft.com/en-au/)
# MAGIC
# MAGIC ## Analytical web apps (for Data Science)
# MAGIC
# MAGIC * [**Dash**](https://dash-gallery.plotly.host/Portal/) building dashboards and data apps in Python (uses Plotly)
# MAGIC * [**streamlit**](https://streamlit.io/) build interactive data apps in Python
# MAGIC * [**anvil**](https://anvil.works/learn/tutorials) build powerful web apps in Python
# MAGIC * [**shiny**](https://shiny.rstudio.com/) interactive web apps in R
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                      |
# MAGIC |-----------------------------------------------------------------|
# MAGIC | <a href="$./6.2 Visualisation" target="_self">Visualisation</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>