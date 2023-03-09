# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. datalake/DimSalesTerritory 디렉토리를 생성한다.
# MAGIC 2. DimSalesTerritory 디렉토리에 dboDimSalesTerritory.parquet 파일을 업로드한다.

# COMMAND ----------

# 자동 로더(Auto Loader)에서 사용할 체크포인트 디렉터리를 생성한다.
account = 'mcloudbridge8'
checkpoint_path = f'abfss://adls@{account}.dfs.core.windows.net/datalake/checkpoint/DimSalesTerritory'
dbutils.fs.mkdirs(checkpoint_path)

# COMMAND ----------

file_path = f'abfss://adls@{account}.dfs.core.windows.net/datalake/DimSalesTerritory'
table_name = 'main.default.DimSalesTerritory'

# PARQUET 파일의 내용을 델타 테이블로 입력하기 위한  Delta table 자동 로더의 구성
(spark.readStream
   .format('cloudFiles')
   .option('cloudFiles.format', 'parquet')
   .option('cloudFiles.schemaLocation', checkpoint_path)
   .option("cloudFiles.schemaEvolutionMode", 'addNewColumns')
   .load(file_path)
   .select('*')
   .writeStream
   .option('checkpointLocation', checkpoint_path)
   .option('mergeSchema', 'true')
   .trigger(availableNow=True)
   .toTable(table_name)
)
