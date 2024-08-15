# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------



# COMMAND ----------

display (dbutils.fs.mounts())

# COMMAND ----------

schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                            StructField("circuitRef", StringType(), False),
                            StructField("name", StringType(), False),
                            StructField("location", StringType(), False),
                            StructField("country", StringType(), False),
                            StructField("lat", DoubleType(), False),
                            StructField("lng", DoubleType(), False),
                            StructField("alt", IntegerType(), False)])

# COMMAND ----------

df = spark.read.csv(path="/mnt/acgmedatalake/1bronze/circuits.csv", header=True, schema=schema)
display(df)

# COMMAND ----------


# df_circuits = df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
df_circuits =df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(df_circuits)

# COMMAND ----------

df_renamed = df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude")

display(df_renamed)

# COMMAND ----------

circuits_final_df = df_renamed.withColumn("ingestion_date", (current_timestamp())) \
    .withColumn('env', lit("Test"))
display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/acgmedatalake/2silver/circuits")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().show()
