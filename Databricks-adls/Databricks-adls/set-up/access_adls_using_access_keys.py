# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using the access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net"))