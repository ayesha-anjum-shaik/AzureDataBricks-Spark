# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q==")
dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

demo_folder_path = "abfss://demo@databrickscoursedludemy.dfs.core.windows.net"
processed_folder_path = "abfss://processed@databrickscoursedludemy.dfs.core.windows.net"
presentation_folder_path = "abfss://presentation@databrickscoursedludemy.dfs.core.windows.net"

# COMMAND ----------

