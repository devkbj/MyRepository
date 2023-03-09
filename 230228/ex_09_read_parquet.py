# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. employees 디렉터리를 생성
# MAGIC 2. employee_4_changedatatype.parquet 파일을 제외한 모든 파일 업로드

# COMMAND ----------

# 파일 목록을 확인
account = 'mcloudbridge8'
directory = 'abfss://adls@{}.dfs.core.windows.net/datalake/employees'.format(account)
dbutils.fs.ls(directory)

# COMMAND ----------

# 하나의 파일을 로드
file_path = '{}/employee_1_origin.parquet'.format(directory)
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

# 여러 파일 목록을 지정하여 로드
files = list()
files.append('{}/employee_1_origin.parquet'.format(directory))
files.append('{}/employee_2_addrow.parquet'.format(directory))

df = spark.read.parquet(*files)
display(df)

# COMMAND ----------

# 디렉터리의 모든 파일을 로드
df = spark.read.parquet(directory)
display(df)

# COMMAND ----------

# 디렉터리의 모든 파일을 로드 - 옵션 변경
df = spark.read\
    .option('mergeSchema', True)\
    .parquet(directory)

display(df)
