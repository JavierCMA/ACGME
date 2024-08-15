# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(),True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points", FloatType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("miliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", StringType(), True),
                            StructField("statusId", StringType(), True),
                            ])

# COMMAND ----------

df = spark.read.json(path='/mnt/acgmedatalake/1bronze/results.json', schema = schema)
display(df)

# COMMAND ----------

df_final = df.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("fastestLap","fastest_lap") \
    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
    .withColumn("ingestion_date",current_timestamp())\
    .drop(col("statusId"))
display(df_final)

# COMMAND ----------

df_final.write.parquet(path="/mnt/acgmedatalake/2silver/results",mode='overwrite',partitionBy='race_id')

# COMMAND ----------


