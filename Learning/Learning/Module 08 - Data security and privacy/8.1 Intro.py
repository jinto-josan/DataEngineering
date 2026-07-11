# Databricks notebook source
# MAGIC %md
# MAGIC ## Data security and privacy 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview
# MAGIC
# MAGIC In the digital age, big data and artificial intelligence (AI) have transformed our lives, generating and processing immense amounts of information. Amidst these advancements, data privacy emerges as a critical concern.
# MAGIC
# MAGIC Data privacy in the context of big data and AI pertains to safeguarding and controlling personal information shared in the digital realm. As we generate vast amounts of data through online activities, preserving its privacy becomes increasingly vital. Unauthorized access or misuse of personal information can unveil intricate details about individuals, jeopardizing personal security, autonomy, and civil liberties.
# MAGIC
# MAGIC Misuse of personal data can lead to discrimination and manipulation. Organizations may exploit user information to target vulnerable populations with biased advertisements or manipulate public opinion through tailored content. Data breaches and cyberattacks pose constant threats, compromising millions of individuals' personal information and exposing them to identity theft and financial fraud.
# MAGIC
# MAGIC Governments and regulatory bodies worldwide have responded by implementing legal frameworks like the General Data Protection Regulation (GDPR) and the California Consumer Privacy Act (CCPA). These regulations aim to ensure individuals' control over personal data and responsible handling by organizations. They emphasize informed consent, transparency in data collection and usage, and granting individuals the right to access and delete their data.
# MAGIC
# MAGIC Data privacy is of utmost importance in the realm of big data and AI, safeguarding personal information, individual autonomy, and promoting a fair and secure digital environment. Preserving and respecting data privacy as a fundamental principle is vital for the responsible and ethical use of data-driven technologies, benefiting society as technology continues to advance. Upholding data privacy allows us to harness the transformative potential of big data and AI while protecting individuals' rights and dignity in the digital age.
# MAGIC
# MAGIC #### So what are the ways in which one anonymize/clean up sensitive data?
# MAGIC * Well, to start with, a good idea would be to remove the sensitive data altogether. However, there are some clients for which is might be an issue as they might not want us to change the data. There comes the next point
# MAGIC * This one is one of the most popular one in the client projects. Just mask or hash (same thing) the data using any programming APIs used in your project. 
# MAGIC * Another option is the provide categories instead of exact values to generalize the data.
# MAGIC * We can also reduce the precision of the data and thereby limiting the information that we are making available. This practice is also known as data minimization.
# MAGIC * We can also add some noise to the data by obscuring the original information, making it harder for unauthorized users to access or understand the data without the correct decryption key.
# MAGIC * Last but not the least, we can totally avoid collecting or storing data that is too sensitive for the clients

# COMMAND ----------

# MAGIC %md
# MAGIC | Next Topic                                                                                                            |
# MAGIC |-----------------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./8.2 Security and Privacy in Data Engineering" target="_self">Security and Privacy in Data Engineering</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>