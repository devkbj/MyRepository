# Databricks notebook source
storage_account = 'mcbedu11'
file_path = 'abfss://adls@{}.dfs.core.windows.net/datalake/dboDimCustomer.parquet'.format(storage_account)
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, asc, desc

# COMMAND ----------

# asc 정렬
display(df.sort('FirstName', 'LastName'))

# COMMAND ----------

# col 함수를 이용한 열 이름 지정
display(df.sort(col('FirstName'), col('LastName')))

# COMMAND ----------

# orderby 이용
display(df.orderBy('FirstName', 'LastName'))

# COMMAND ----------

# desc 정렬
display(df.sort(col('FirstName').desc(), col('LastName').asc()))
