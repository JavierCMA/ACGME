# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                            StructField("driverId", IntegerType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("position", IntegerType(),True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            ])

# COMMAND ----------

df = spark.read.csv(path="/mnt/acgmedatalake/1bronze/lap_times",schema=schema)
display(df)

# COMMAND ----------

df_final = df.withColumnRenamed("driverId","driver_id")\
            .withColumnRenamed("raceId","race_id")\
            .withColumn("ingestion_date", current_timestamp())
display(df_final)

# COMMAND ----------

df_final.write.parquet(path="/mnt/acgmedatalake/1bronze/lap_times",mode="overwrite")

# COMMAND ----------


