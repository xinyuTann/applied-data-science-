# Databricks notebook source
df_laptimes= spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv',header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers= spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv',header=True)
df_drivers.count()

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transform Data

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date,avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_drivers=df_drivers.withColumn("age",datediff(current_date(),df_drivers.dob)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers=df_drivers.withColumn("age", df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

df_lap_drivers=df_drivers.join(df_laptimes,on=['driverId'])

# COMMAND ----------

df_lap_drivers=df_drivers.select('driverId', 'nationality','age','forename','surname','url').join(df_laptimes,on=["driverId"])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregate By Age

# COMMAND ----------

df_lap_drivers=df_lap_drivers.groupBy('nationality', 'driverId','age').agg(avg('milliseconds'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Storing data in S3

# COMMAND ----------

df_lap_drivers.write.csv('s3://xt2267-gr5069/processed/in_class_workshop/laptimes_by_drivers.csv')
