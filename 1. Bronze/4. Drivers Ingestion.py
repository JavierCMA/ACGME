# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                            StructField("surname", StringType(), True)])
                            

# COMMAND ----------


schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("code", StringType(), True),
                            StructField("name", name_schema),
                            StructField("dob", DateType(), True),
                            StructField("nationality", StringType(), True),
                            StructField("url", StringType(), True)
                            ])

# COMMAND ----------

df = spark.read.json(path="/mnt/acgmedatalake/1bronze/drivers.json", schema = schema)
display(df)

# COMMAND ----------

df_final = df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("name", concat(col("name.forename"), lit(' '),col("name.surname")))
display(df_final)



# COMMAND ----------

df_final.write.parquet(path ='/mnt/acgmedatalake/2silver/drivers', mode ='overwrite')

# COMMAND ----------


