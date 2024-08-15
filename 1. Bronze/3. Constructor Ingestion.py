# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                            StructField("ConstructorRef", StringType(), False),
                            StructField("name", StringType(), False),
                            StructField("nationality", StringType(), False),
                            StructField("url", IntegerType(), False)])

# COMMAND ----------

df = spark.read.json(path="/mnt/acgmedatalake/1bronze/constructors.json")
display(df)

# COMMAND ----------

df = (df.drop('url')
.withColumnRenamed ("constructorId","constructor_id") 
.withColumnRenamed ("consrtuctoRef","constructor_ref") 
.withColumn("ingestion_date", current_timestamp()) )

display(df)



# COMMAND ----------

df.write.parquet(path="/mnt/acgmedatalake/2silver/constructors", mode='overwrite')

# COMMAND ----------


