# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of dbutils.secret utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'databrickscourse-secretscope')

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'databrickscourse-secretscope', key = 'databrickscourse-access-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")