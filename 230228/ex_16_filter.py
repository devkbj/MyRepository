# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. dw 폴더의 모든 parquet 파일을 업로드한다.

# COMMAND ----------

account = 'mcloudbridge8'
file_path = 'abfss://adls@{}.dfs.core.windows.net/datalake/dboDimCustomer.parquet'.format(account)
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

# 성별이 여자(F)인 항목 필터
df.filter(df.Gender == 'F').show(truncate=False)

# COMMAND ----------

# 열 이름으로 필터
from pyspark.sql.functions import col
df.filter(col('Gender') == 'F').show(truncate=False)

# COMMAND ----------

# 같지 않은 항목 필터
#df.filter(~(df.TotalChildren == 0)).show(truncate=False)
df.filter(df.TotalChildren != 0).show(truncate=False)

# COMMAND ----------

# 두 조건을 모두 만족(and)
display(df.filter( (df.Gender  == 'F') & (df.TotalChildren  > 0) ))

# COMMAND ----------

# 두 조건중 하나라도 만족(or)
display(df.filter( (df.Gender  == 'F') | (df.TotalChildren  > 0) ))

# COMMAND ----------

# isin 연산
lst = ['Professional', 'Management']
display(df.filter(df.EnglishOccupation.isin(lst)))

# COMMAND ----------

# 문자열이 ~으로 시작. 대소문자를 구분 한다.
display(df.filter(df.FirstName.startswith('Ka')))

# COMMAND ----------

# 문자열이 ~으로 종료
display(df.filter(df.FirstName.endswith('er')))

# COMMAND ----------

# 문자열 포함
display(df.filter(df.FirstName.contains('the')))
