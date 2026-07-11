# Databricks notebook source
# MAGIC %md
# MAGIC # Data Mesh
# MAGIC
# MAGIC In this lesson, you will explore Data Products and the principles of the Data Mesh concept, along with gaining valuable references to deepen your understanding of the topic.
# MAGIC
# MAGIC As you will see, Data Mesh, despite sounding like a technical term at first, is an organizational concept which provides guidelines towards a more or better data-driven organization.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe what Data Mesh is and its relevance
# MAGIC * Know about the Data Mesh principles and what it's required to have implemented
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is Data Mesh?
# MAGIC
# MAGIC Watch the video **[Introduction to Data Mesh by Zhamak Dehghani](https://www.youtube.com/watch?v=_bmYXWCxF_Q)**
# MAGIC
# MAGIC 💡 **REMEMBER:**
# MAGIC
# MAGIC > “Data Mesh is a decentralized sociotechnical approach to share, access, and manage analytical data in complex and large-scale environments within or across organizations.” -- [Zhamak Dehghani](https://www.linkedin.com/in/zhamak-dehghani/)
# MAGIC
# MAGIC **Data Mesh** calls for a **multi-dimensional shift**, both **technically**, and **organisationally**. It moves the organization from centralized ownership to decentralized ownership along **domain boundaries**. It shifts the architecture from a monolithic data system, to a **distributed set of analytical products**. Technically it takes teams from treating their data as a side-effect or byproduct, to **_treating the data as a first class citizen and product_**. It pushes for an operational shift from top-down governance to **_federated and computational governance_**. It moves from the principal of having data as a thing to collect and store, to **_data as a product to share, connect, and evolve_**. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Mesh approach
# MAGIC
# MAGIC <!--<div style={{textAlign: 'center'}} > 
# MAGIC <figure class="video-container">
# MAGIC     <iframe src="https://www.youtube.com/embed/E7KqqxSVh4A" id="ytplayer"  allowtransparency="true" frameborder="0" scrolling="no" allowfullscreen="allowfullscreen" mozallowfullscreen webkitallowfullscreen oallowfullscreen msallowfullscreen width="620" height="349"></iframe>
# MAGIC </figure>
# MAGIC </div>
# MAGIC -->
# MAGIC Watch the video **[Data Mesh Overview by Darren Young](https://www.youtube.com/embed/E7KqqxSVh4A)
# MAGIC
# MAGIC The approach aims to address some of these issues through the application of the following four principles:
# MAGIC
# MAGIC - **Domain-driven** ownership of data
# MAGIC - Treating **data as a product**
# MAGIC - **Enabling data owners** with a **self-serve** data platform
# MAGIC - **Federated**, computational **data governance**
# MAGIC
# MAGIC #### Domain-driven ownership of data
# MAGIC
# MAGIC **Data should be managed close to its source of origin**. Data Mesh achieves this through a distributed domain driven
# MAGIC architecture which aligns data with the domain boundaries of an organization.  This allows us to scale, by moving 
# MAGIC away from monolithic analytical architectures / team structures which can become a bottleneck, and **close the gap**
# MAGIC between **data producers and their consumers**. It also helps to establish proper ownership and accountability for data.
# MAGIC
# MAGIC In a traditional setup, we might expect to see an organization segment their operational and analytical spaces quite
# MAGIC rigidly. Unfortunately this ends up creating a centralized (and monolithic) central analytics function. Instead of
# MAGIC having systems and data segmented along domain boundaries, a centralized approach causes friction where teams are 
# MAGIC siloed (e.g. a central analytics team). To know more about watch the following optional [video](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-mesh-and-domain-ownership).
# MAGIC
# MAGIC <!--
# MAGIC <div style={{textAlign: 'center'}} > 
# MAGIC <figure class="video-container">
# MAGIC     <iframe src="//fast.wistia.net/embed/iframe/rqa9599gof" allowtransparency="true" frameborder="0" scrolling="no" class="wistia_embed" name="wistia_embed" allowfullscreen="allowfullscreen" mozallowfullscreen webkitallowfullscreen oallowfullscreen msallowfullscreen width="620" height="349"></iframe>
# MAGIC </figure>
# MAGIC *Danilo and Zhamak explore the principle of domain ownership in this [webinar](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-mesh-and-domain-ownership)*
# MAGIC </div>
# MAGIC -->
# MAGIC
# MAGIC #### Treating Data as a Product
# MAGIC
# MAGIC An architectural quantum is the smallest unit of architecture that can be independently deployed
# MAGIC with **high functional cohesion** and containing all of the elements required for it to function. 
# MAGIC
# MAGIC The architectural quantum of a Data Mesh is a Data Product.
# MAGIC
# MAGIC A Data product provides a **representation of analytical data** served for a **particular purpose** within the analytical plane. **Ownership exists within the domain**: the same teams that own the operational systems that produce the data are also responsible for this analytical product bringing their data into the Data Mesh. As with any other product, a data product should be **usable**, **valuable**, and **feasible**.
# MAGIC
# MAGIC For a **Data product** to be **usable**, it should be:
# MAGIC
# MAGIC - Discoverable
# MAGIC - Understandable
# MAGIC - Trustworthy
# MAGIC - Secure
# MAGIC - Reusable
# MAGIC - Interoperable
# MAGIC - Accessible
# MAGIC
# MAGIC For it to be **valuable**, we should ensure that the product meets a real business need. \"Data Product\" should not be conflated with \"data set\". To know more about watch the following optional [video](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-as-a-product).
# MAGIC
# MAGIC <!--
# MAGIC <div style={{textAlign: 'center'}} > 
# MAGIC <figure class="video-container">
# MAGIC     <iframe src="//fast.wistia.net/embed/iframe/zed92ui3r7" allowtransparency="true" frameborder="0" scrolling="no" class="wistia_embed" name="wistia_embed" allowfullscreen="allowfullscreen" mozallowfullscreen webkitallowfullscreen oallowfullscreen msallowfullscreen width="620" height="349"></iframe>
# MAGIC </figure>
# MAGIC *In this [webinar](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-as-a-product)*
# MAGIC Zhamak covers the principle of treating data as a product
# MAGIC </div>
# MAGIC -->
# MAGIC
# MAGIC #### Data Mesh Platform
# MAGIC
# MAGIC If we want to treat data as a product and allow teams to not only produce data but also create and own this new type of analytical data products then we need to put the constructs in place to empower those teams and reduce the cost of ownership and risk of reinventing the wheel between domains. **Providing a platform** through which teams can **self-serve their data products** will also allow some of the standards and computational governance to be baked into any of the data products being brought into the Data Mesh.
# MAGIC
# MAGIC The **Data Mesh platform** will provide **standardized tooling** and **domain agnostic** capabilities for data products. 
# MAGIC Onboarding Data Products onto the Data Mesh Platform enables the exchange of value between producers and consumers through a defined set of interfaces (in the data product). It can also provide the guard rails to ensure that the data products develop in a standardized manner, thereby enabling **interoperability**, **consistency**, **reduced complexity**. To know more about watch the following optional [video](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-platform-in-a-mesh-architecture).
# MAGIC
# MAGIC ### Federated, computational data governance
# MAGIC
# MAGIC Data Mesh is decentralized at its core. There is however a need for us to have certain policies and structures in place to guide the evolution of a decentralized system and ensure that particular standards are met. Governance in the Data, Mesh, however, moves away from policies being defined in isolation and towards building the policies needed into the data products and platform where possible. This **federated and computational governance** will **ensure consistent and reliable enforcement of policies** across the Data Mesh ecosystem, and enable higher order value by allowing data products to **interoperate effectively**. To know more about it, watch the following optional [video](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh/data-mesh-and-governance#recording).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additional References (Optional)
# MAGIC
# MAGIC Despite the fact that Data Mesh is a key and trending topic in the data lanscape, and for TW offerings strategy too. We won't go in deep in this course. 
# MAGIC
# MAGIC #### Campus Course
# MAGIC
# MAGIC - We really recommend our official course [Data Mesh Intro](https://campus.thoughtworks.net/learn/courses/80420/data-mesh-intro?hash=f5faad6021a66cec16771c84ee50f752fbbd1165&generated_by=14019)
# MAGIC
# MAGIC #### Articles
# MAGIC
# MAGIC - [How to Move Beyond a Monolithic Data Lake to a Distributed Data Mesh](https://martinfowler.com/articles/data-monolith-to-mesh.html)
# MAGIC - [Principles and Logical Architecture](https://martinfowler.com/articles/data-mesh-principles.html)
# MAGIC - [Testing a Data Product in Data Mesh](https://blog.thoughtworks.net/ankur-jain/testing-a-data-product-in-data-mesh)
# MAGIC - [Data Mesh Reference Implementation](https://www.researchgate.net/publication/371399084_Decentralized_Data_Governance_as_Part_of_a_Data_Mesh_Platform_Concepts_and_Approaches)
# MAGIC - [The four principles of Data mesh](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh)
# MAGIC - [It's not about Tech, It's about Ownership](https://www.thoughtworks.com/en-in/insights/blog/data-mesh-its-not-about-tech-its-about-ownership-and-communication).
# MAGIC
# MAGIC #### Videos
# MAGIC
# MAGIC - [Data Mesh Context](https://www.youtube.com/embed/j9dJ2TPowFc)
# MAGIC - [Data Mesh - Analytical and Operational Data ](https://www.youtube.com/watch?v=AqZ3Hhnr-cY)
# MAGIC - [When design meets data: Using Data Mesh for a consumer and business-driven data strategy – YConf](https://www.youtube.com/embed/uIDy7GKYv9E)
# MAGIC - [The four principles of Data Mesh - Webinar Series](https://www.thoughtworks.com/about-us/events/webinars/core-principles-of-data-mesh)
# MAGIC - [Data Mesh: Unlocking Data Value in the Enterprise](https://fast.wistia.net/embed/iframe/7uegix6aqe) 
# MAGIC
# MAGIC
# MAGIC #### Books
# MAGIC
# MAGIC <div style={{textAlign: 'center'}}>
# MAGIC
# MAGIC [<img src='https://learning.oreilly.com/library/cover/9781492092384/350h/'/>](https://www.oreilly.com/library/view/data-mesh/9781492092384/) &nbsp;&nbsp;
# MAGIC [<img src='https://learning.oreilly.com/library/cover/9781098108502/350h/'/>](https://www.oreilly.com/library/view/data-mesh-in/9781098108502/)
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Wrap-up
# MAGIC
# MAGIC Congratulations, you have finished the Data Architecture module. As of now you should know about different data architectures, data platforms and products, and Data Mesh. You should have started coding with Spark in your Notebooks and you should have been ready to keep learning and coding more in Spark and Python, for sure you'll have the chance to keep coding and learning in the upcoming modules!
# MAGIC
# MAGIC We really wish that you keep enjoying this learning journey! Kudos for you!

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                    | Next Module                                                                                                                                          | 
# MAGIC |-------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./3.1 Data Platforms" target="_self">Data Platforms</a> | <a href="$./3.3 Practice Domain Introduction" target="_self">Practice Domain Introduction</a>  | 

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>