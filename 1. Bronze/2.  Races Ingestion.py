# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                            StructField("year", IntegerType(), False),
                            StructField("round", IntegerType(), False),
                            StructField("circuitId", IntegerType(), False),
                            StructField("name", StringType(), False),
                            StructField("date", DateType(), False),
                            StructField("time", DoubleType(), False),
                            StructField("url", IntegerType(), False)])

# COMMAND ----------

df = spark.read.csv(path="/mnt/acgmedatalake/1bronze/races.csv", header=True)
display(df)

# COMMAND ----------

df_final = df.withColumn("ingestion_date", current_timestamp()).withColumn(
    "race_timestamp",
    to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')
)
display(df_final)

# COMMAND ----------

df_final1 = df_final.select(
    col('raceId').alias("race_id"),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias("circuit_id"),
    col('name'),
    col('ingestion_date'),
    col('race_timestamp')
)
display(df_final1)

    

# COMMAND ----------

df_final1.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/acgmedatalake/2silver/races')

# COMMAND ----------

dbutils.notebook.exit('Success_Races')
