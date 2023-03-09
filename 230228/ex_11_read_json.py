# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. employee.json 파일 업로드
# MAGIC 2. menu.json 파일 업로드
# MAGIC 3. nvshop.json 파일 업로드

# COMMAND ----------

# 파일 목록을 확인
account = 'mcbtest'
directory = 'abfss://adls@{}.dfs.core.windows.net/datalake'.format(account)
dbutils.fs.ls(directory)

# COMMAND ----------

# MAGIC %md
# MAGIC # employee.json

# COMMAND ----------

# json 파일을 로드한다.
file_path = '{}/employee.json'.format(directory)
df = spark\
    .read\
    .json(file_path.format(file_path))

display(df)

# COMMAND ----------

# multiLine 옵션을 활성화한다
df = spark\
    .read\
    .option('multiline', True)\
    .json(file_path)

display(df)

# COMMAND ----------

# 스키마 확인
df.printSchema()

# COMMAND ----------

# 데이터 조회 - 열
df.select('employees.name', 'employees.email').show(truncate=False)

# COMMAND ----------

# 데이터 조회 - 행
from pyspark.sql.functions import col

df.select(col('employees.name')[0], col('employees.email')[0]).show(truncate=False)

# COMMAND ----------

# Array 형식 값을 행으로 변환
from pyspark.sql.functions import explode

df_json = df.select(explode('employees').alias('employee'))
df_json = df_json.select('employee.email', 'employee.name')
display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC # menu.json

# COMMAND ----------

# json 파일을 로드한다.

file_path = '{}/menu.json'.format(directory)
df = spark\
    .read\
    .option('multiline', True)\
    .json(file_path.format(file_path))

display(df)
# Error원인 : json형식은 대소문자를 구분 X

# COMMAND ----------

# json 파일을 로드한다.
file_path = '{}/menu_fixed.json'.format(directory)
df = spark\
    .read\
    .option('multiline', True)\
    .json(file_path.format(file_path))

display(df)

# COMMAND ----------

# 데이터 조회
df.select('menu.id', 'menu.ID2', 'menu.value', 'menu.popup').show(truncate=False)

# COMMAND ----------

# 데이터 조회
df.select('menu.id', 'menu.ID2', 'menu.value', 'menu.popup.menuitem').show(truncate=False)

# COMMAND ----------

# ARRAY 형식의 값을 행으로 변환(explode)
df.select('menu.id', 'menu.ID2', 'menu.value', explode('menu.popup.menuitem').alias('menuitem')).show(truncate=False)

# COMMAND ----------

# 최종 데이터 가공
df_menu = df\
    .select('menu.id', 'menu.ID2', 'menu.value', explode('menu.popup.menuitem').alias('menuitem'))\
    .select('id', 'ID2', 'value', col('menuitem.value').alias('menu_value'), 'menuitem.onclick')

display(df_menu)

# COMMAND ----------

# MAGIC %md
# MAGIC # nvshop.json

# COMMAND ----------

# json 파일을 로드한다.
file_path = '{}/nvshop.json'.format(directory)
df = spark\
    .read\
    .option('multiline', True)\
    .json(file_path.format(file_path))

display(df)
