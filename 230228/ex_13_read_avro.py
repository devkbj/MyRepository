# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. sensor.avro 파일 업로드

# COMMAND ----------

# 파일 목록을 확인
account = 'mcloudbridge8'
directory = 'abfss://adls@{}.dfs.core.windows.net/datalake'.format(account)
dbutils.fs.ls(directory)

# COMMAND ----------

# 파일 로드
file_path = '{}/sensor.avro'.format(directory)
df = spark.read\
    .format('avro')\
    .load(file_path)

display(df)

# COMMAND ----------

# Body 열의 값을 문자열로 변환
df_avro = df.select(df.Body.cast('string'))
display(df_avro)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
schema = StructType([ 
    StructField("SensorID",IntegerType(),True), 
    StructField("Temperature",IntegerType(),True), 
    StructField("Time",TimestampType(),True), 
    StructField("Status", StringType(), True)
  ])

from pyspark.sql.functions import col, from_json
df_result = df_avro\
            .withColumn('jsonData', from_json(col('Body'), schema))\
            .select("jsonData.*")
df_result.printSchema()
display(df_result)
