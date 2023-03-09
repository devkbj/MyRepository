# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. ProductLT_Header.csv 파일과 ProductLT_NoHeader.csv 파일 업로드

# COMMAND ----------

# 파일 목록을 확인
account = 'mcbtest'
directory = 'abfss://adls@{}.dfs.core.windows.net/datalake'.format(account)
dbutils.fs.ls(directory)

# COMMAND ----------

# 헤더가 함께 저장된 csv 파일을 로드한다.
file_path = '{}/ProductLT_Header.csv'.format(directory)
df = spark.read.csv(file_path)
display(df)

# COMMAND ----------

# .option('header', True)\ : 헤더 옵션 지정
df = spark.read\
    .option('header', True)\
    .csv(file_path)

display(df)

# COMMAND ----------

# 데이터 형식 확인
df.printSchema()

# COMMAND ----------

# .option('inferSchema', True)\ : 스키마를 자동으로 유추
df = spark.read\
    .option('header',True)\
    .option('inferSchema', True)\
    .csv(file_path)

df.printSchema()

# COMMAND ----------

# 스키마 직접 지정
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import IntegerType, StringType, DateType
from pyspark.sql.types import *

schema = StructType([
StructField('ProductID',IntegerType()),
StructField('Name',StringType()),
StructField('ProductNumber',StringType()),
StructField('SafetyStockLevel',IntegerType()),
StructField('ReorderPoint', IntegerType()),
StructField('ListPrice', StringType()),##
StructField('SellStartDate', DateType()),
StructField('SubcategoryName', StringType()),
StructField('CategoryName', StringType())
])

df = spark.read\
    .option('header',True)\
    .schema(schema)\
    .csv(file_path)

df.printSchema()
display(df)  

# COMMAND ----------

# 헤더가 없는 파일 로드
file_path = '{}/ProductLT_NoHeader.csv'.format(directory)
df = spark.read\
    .csv(file_path)

display(df)

# COMMAND ----------

# 스키마 지정


# schema = StructType([
# StructField('ProductID',IntegerType()),
# StructField('Name',StringType()),
# StructField('ProductNumber',StringType()),
# StructField('SafetyStockLevel',IntegerType()),
# StructField('ReorderPoint', IntegerType()),
# StructField('ListPrice', StringType()),##
# StructField('SellStartDate', DateType()),
# StructField('SubcategoryName', StringType()),
# StructField('CategoryName', StringType())
# ])


## Cmd 7에서 선언한  값이 schema에 아직 남아 있음
# 주의할점 : 헤더순서와 StructType순서가 동일해야함
df = spark.read\
    .schema(schema)\
    .csv(file_path)

display(df)
