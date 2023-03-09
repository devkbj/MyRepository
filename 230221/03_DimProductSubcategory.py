# Databricks notebook source
# 자동 로더(Auto Loader)에서 사용할 체크포인트 디렉터리를 생성한다.
username = spark.sql("SELECT current_user()").first()[0]
username = username.split('@')[0]

checkpoint_path = 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/checkpoint/{}/DimProductSubcategory'.format(username)
dbutils.fs.mkdirs(checkpoint_path)

# COMMAND ----------

file_path = 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/DimProductSubcategory'
table_name = 'main.{}.DimProductSubcategory'.format(username)

# PARQUET 파일의 내용을 델타 테이블로 입력하기 위한 Delta table 자동 로더의 구성
(spark.readStream
   .format('cloudFiles')
   .option('cloudFiles.format', 'parquet')
   .option('cloudFiles.schemaLocation', checkpoint_path)
   .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
   .load(file_path)
   .select('*')
   .writeStream
   .option('checkpointLocation', checkpoint_path)
   .option('mergeSchema', 'true')
   .trigger(availableNow=True)
   .toTable(table_name)
)
