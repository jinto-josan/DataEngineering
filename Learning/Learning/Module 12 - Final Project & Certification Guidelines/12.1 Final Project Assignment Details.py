# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to the final project
# MAGIC
# MAGIC During the course, you've probably been thinking about all the amazing things you could do with your newly acquired knowledge. Now it's time to bring it all together and impress each other with project implementations of your own using all the knowledge you have gained this far.
# MAGIC
# MAGIC > Pairing and working in a group is mandatory!
# MAGIC
# MAGIC The topic you will be working on is entirely up to your team. Sometimes you will already have a topic in mind and will have to attract additional participants to join your team, in other cases you might already have a team and can focus on finding a topic that is interesting to everyone.
# MAGIC
# MAGIC For organizing the team and topic composition, the trainer(s) will choose an appropriate way of communication. Options range from using the chat, creating a mural to creating dedicated Google sheets.
# MAGIC This activity can start a few weeks before the final presentation and can act as a good reference for both the learners and the trainers to plan the final assignment implementation and presentation.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final project schedule
# MAGIC Please find the steps to be followed for the final assignment completion below:
# MAGIC
# MAGIC ### Before the final project (typically during Modules 10 & 11):
# MAGIC - Find a good dataset and promote it in the chat space to form a team of 2 - 3 members. Some links are provided in `pick a dataset` section below but feel free to bring your own dataset as well if you find it more interesting and relevant. Note that private datasets from client projects are not allowed to be worked with.
# MAGIC - Document your teams as soon as they are finalized.
# MAGIC - Prepare and document a management summary. Some examples are given in the `Management Summary Examples` section below.
# MAGIC - Construct a business case around your management summary with questions which should be answered from this dataset. Document those questions as well in your management summaries. Please look at the `Business Case` section below for details.
# MAGIC - The goal is for you and your team to have a clear path for working on your final project and not lose time with figuring out what exactly you want to do during the precious project time.
# MAGIC - Book a time slot for a **30 minutes presentation** with your trainers. Your trainers will communicate how to do this. Depending on the format of the course, this will happen on the final project day or be a separate appointment with the trainers.
# MAGIC
# MAGIC ### During the final project (Module 12):
# MAGIC - Start the implementation. It is up to you when and how you work on your final project during the designated project time. We recommend pairing on the final project. However, if you choose to work in asynchronous fashion as a team that is also acceptable. Please just adhere to the allocated project time of 6-8 hours.
# MAGIC - Ingest the data and clean it up properly. Implement quality gates to make sure everything looks fine for further transformations.
# MAGIC - Data quality will be assessed during evaluation—refer to <a href="$../Module 05 - Introducing the Multi-Hop Architecture/5.6 Data-Quality" target="_self">5.6 Data Quality</a> for guidance.
# MAGIC - Transform the data into the target structure that enables you to answer the business questions documented in your management summary adequately.
# MAGIC - Usage of the multi hop (medallion) architecture is highly recommended.
# MAGIC - The preparation of the final presentation also falls within the time frame. A slide deck or any other format you prefer to present your findings is accepted.
# MAGIC
# MAGIC ### Delivering the final project presentation
# MAGIC Great, your implementation is complete and you have already booked a 30 minute presentation timeslot with your trainers. Here are some guidelines for the presentation:
# MAGIC
# MAGIC - Prepare for the presentation before coming to the presenation session.
# MAGIC - Presentation structure: Slides are not compulsory but they can be helpful for structuring your content. We typically see better results for teams that took the time to produce a slide deck.
# MAGIC - Introduce the audience to your management summary and the questions you are going to answer.
# MAGIC - Present the transformation journey you took to answer the questions.
# MAGIC - Present the solution in an easy to understand manner (business audience).
# MAGIC - Present interesting parts of your code and talk about the challenges you faced.
# MAGIC - Make sure that all your team members participate and talk in the presentation to showcase their work.
# MAGIC - Please keep the duration of your presentation to 15-20 minutes to allow the remaining time for Q&A and discussions.
# MAGIC
# MAGIC ### Final Trainer Connect meeting
# MAGIC Sometimes, the final presentations will be done with the whole group on the final project day. This way, everyone gets to see what all teams have been working on and it automatically marks the end of the training.
# MAGIC
# MAGIC In other cases and with a growing number of participants, going through all the presentations with everyone can be very long and frankly quite tough to schedule. In this case, we choose to schedule individual presentation slots for the teams.
# MAGIC To make sure everyone gets at least a small glimpse of what all the other teams have been working on, we invite everyone to a final Trainer Connect Meeting after the individual presentations are done. Here, all teams present their projects in a summary of 5-10 minutes, followed by a short Q&A. This meeting will also mark the completion of your training. Celebrate, enjoy and give yourself a pat on the back for completing this training!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Picking a dataset
# MAGIC
# MAGIC At these resources you can find a lot of curated datasets. You can also use your own data.
# MAGIC
# MAGIC - https://dev.to/seattledataguy/5-data-sources-for-data-engineering-projects-3j21
# MAGIC - https://github.com/awesomedata/awesome-public-datasets
# MAGIC - https://www.tableau.com/learn/articles/free-public-data-sets
# MAGIC - https://www.google.com/publicdata/directory
# MAGIC - https://ourworldindata.org (/world-population-growth)
# MAGIC - https://open.esa.int
# MAGIC - https://www.kaggle.com/datasets
# MAGIC
# MAGIC You are free to use other sources of data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a business case
# MAGIC
# MAGIC Write a management-summary of the goals to achieve. This means we first look for data, and then we build a business case. This is the opposite of what you should do in real life, but in the interest of time we're doing it this way around.
# MAGIC
# MAGIC Your management summary should typically be a document with a small introduction of the dataset followed by specific questions you like to answer and implement solutions for. Please narrow it down to few and specific questions over exploring the data and seeing what comes out of it. We typically see better results for teams that manage to do this well and check early if their dataset actually contains the right data and enough data to fullfil the business case.
# MAGIC
# MAGIC **Share your management-summary with your trainers.**
# MAGIC This is a important step, do not miss it. Ask your trainers how you can share the summaries.
# MAGIC
# MAGIC Advertise your idea to find pairs working with you. To work in a triple or a pair is compulsory.
# MAGIC

# COMMAND ----------

Ingest the data and clean it up properly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Management Summary Examples
# MAGIC
# MAGIC Below are a few examples of management summaries you can have a look at for inspiration.
# MAGIC
# MAGIC ### Food waste
# MAGIC We want to take a look at (staple) food prices and how they might correlate with the amount of food being thrown away. Our hypothesis: an increase of food prices will reduce the amount of wasted food. For that, we will look into data for food production prices, retail prices, organic household waste, and income, provided by the Federal Statistic Office of Germany.
# MAGIC Data: Destatis Genesis platform (61131-0001, 61211-0001, 32121-0001, [12211-0008, 12241-9000, 62361-0020)
# MAGIC Team: Justus, Peter, Bob
# MAGIC
# MAGIC
# MAGIC ### Bike Rental
# MAGIC Bike Rental Different types of bike hire customers have different preferences, but all would want serviceable bikes during busy hours. We have identified commuters as potential focus target market, and we want to service them well. In order to do this, we want to identify which stations are used more regularly by commuters using usage patterns during rush hour and proximity to certain business areas, in order to identify which stations may need extra maintenance and whether to advertise to commuters.
# MAGIC Data: Chicago Bicycle Rent Usage
# MAGIC Team: Me, (looking for a second and a third person)

# COMMAND ----------

# MAGIC %md
# MAGIC ## While you work on the implementation
# MAGIC
# MAGIC So by now you are ready with your management summary and business case and would have started the implementation as well. A few things to be kept in mind and taken care of while you are doing that :
# MAGIC
# MAGIC - Copy all notebooks you have worked on as a team to your private folder after the presentation: in the `final-project` folder. Ask you trainer for the location of this folder.
# MAGIC - Comment your work with a summery and an author tag:
# MAGIC <pre>
# MAGIC /// Project: The rise of individual impacts on global warming
# MAGIC /// Summary: We strive to demonstrate the extent of the impact an  has made towards global warming in recent years,
# MAGIC     in comparison to the preceding decade. We establish a connection between the global population and global warming,
# MAGIC     and illustrate the corresponding carbon dioxide (CO2) emissions on a per-country and per-capita basis.
# MAGIC /// Data   : world-population, global-land-temp, CO2-emission
# MAGIC /// Authors: Justus, Peter and Bob
# MAGIC </pre>
# MAGIC - Project organisation: please make sure that your project is organised well. We don't need long documentation in the code, but it would be great if it's properly commented at the very least
# MAGIC - Self Test & Validate your data. The tests and solutions should not fail in the presentation
# MAGIC - Lastly, please look at the `Useful tip to share your notebook in the team` section below to enable sharing notebooks across your team for better collaboration.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Useful tip to share your notebook in the team
# MAGIC
# MAGIC While pairing on the final implementation day, you might want to share your cluster with your pair to seemlessly work on the same notebook with your pair together. This can be enabled in the cluster as follows
# MAGIC
# MAGIC 1. Go to cluster configuration of your cluster and click on click on `Edit` button to edit the configuration of your cluster.
# MAGIC 2. Now to to the `Advanced` section down below on this page.
# MAGIC 3. Select the `Access Mode` here and then select the `Manual` option.
# MAGIC 4. You will see a dropdown just infront of the `Manual` option . Click on it.
# MAGIC 5. Select `No Isolation Shared` option from the drop down. You might have to click on `3 more` link in the drop down to see more options.
# MAGIC
# MAGIC This will enable sharing of the notebooks accoss members of the pair.

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                         |
# MAGIC |----------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./12.2 Databricks Certification Guidelines" target="_self">Databricks Certification Guidelines</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>
# MAGIC