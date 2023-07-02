# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using the sas token
# MAGIC 1. Set the spark config using SAS token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickscoursedludemy.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickscoursedludemy.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickscoursedludemy.dfs.core.windows.net", "sp=rl&st=2023-07-01T03:04:01Z&se=2023-07-01T11:04:01Z&spr=https&sv=2022-11-02&sr=c&sig=KwXL3km%2FMd%2BfCiThnnz%2FYje9hn%2Bb14b6oY4lz3exl7c%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net"))