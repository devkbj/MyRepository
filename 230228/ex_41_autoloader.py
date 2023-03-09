# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. Azure Storage에서 datalake/checkpoint 디렉토리를 만든다.
# MAGIC 2. employees 디렉터리에서 employee_1_origin.parquet를 제외한 모든 파일을 삭제한다.

# COMMAND ----------

account = 'mcloudbridge8'
external_location = 'abfss://adls@{}.dfs.core.windows.net/datalake'.format(account)
table = 'main.default.employee'
checkpoint_path = '{}/checkpoint'.format(external_location)
file_path = '{}/employees'.format(external_location)

# COMMAND ----------

# 테이블이 존재하면 삭제한다
sql = f'DROP TABLE IF EXISTS {table}'
spark.sql(sql)

# COMMAND ----------

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
 .toTable(table))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.employee

# COMMAND ----------

sql = f"SELECT * FROM cloud_files_state('{checkpoint_path}')"
df = spark.sql(sql)
display(df)
