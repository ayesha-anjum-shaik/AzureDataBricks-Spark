# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload files to DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------

