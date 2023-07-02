# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

circuits_df.select(count("*")).show()

# COMMAND ----------

circuits_df.select(countDistinct("location")).show()

# COMMAND ----------

circuits_df.select(sum("altitude")).show()

# COMMAND ----------

circuits_grouped_df = circuits_df.groupBy("location").agg(sum("altitude").alias("altitudes_sum"),countDistinct("circuit_ref").alias("circuitsref_count"))

# COMMAND ----------

display(circuits_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

circuit_rank = Window.partitionBy("location").orderBy(desc("altitudes_sum"))
circuits_grouped_df.withColumn("rank",rank().over(circuit_rank)).show()

# COMMAND ----------

