# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using the cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from the demo container
# MAGIC 1. Read contents from the circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net"))