# Databricks notebook source
# MAGIC %run "../workflows/paths"

# COMMAND ----------

bronze_path

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(),True),
                            StructField("number", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("q1", StringType(), True),
                            StructField("q2", StringType(), True),
                            StructField("q3", StringType(), True),
                            ])

# COMMAND ----------

df= spark.read.json(path=f"{bronze_path}/qualifying", schema=schema, multiLine=True)
display(df)

# COMMAND ----------

df_final = df.withColumnRenamed("qualifyId","qualify_id")\
            .withColumnRenamed("driverId","driver_id")\
            .withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("constructorId","constructor_id")\
            .withColumn("ingestion_date", current_timestamp())
display(df_final)

# COMMAND ----------

df_final.write.parquet(path=f"{silver_path}/qualifying",mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit('Success_Qualifying')
