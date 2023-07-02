# Databricks notebook source
# MAGIC %md
# MAGIC #### Read constructors.json file using spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q==")
dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted columns from dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_renamed_df = constructor_df.withColumnRenamed("constructorId","constructor_id") \
                                        .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to parquet file

# COMMAND ----------

constructor_renamed_df.write.mode("overwrite").parquet("abfss://processed@databrickscoursedludemy.dfs.core.windows.net/constructors")

# COMMAND ----------

