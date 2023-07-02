# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q==")
dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")
circuits_df = spark.read.option("header",True).csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_filtered_df = circuits_df.filter((circuits_df["location"]=="Kuala Lumpur") | (circuits_df["alt"]>10))

# COMMAND ----------

display(circuits_filtered_df)

# COMMAND ----------

