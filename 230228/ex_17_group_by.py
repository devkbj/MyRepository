# Databricks notebook source
storage_account = 'mcbedu11'
file_path = 'abfss://adls@{}.dfs.core.windows.net/datalake/dboDimCustomer.parquet'.format(storage_account)
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

# 열 하나의 (행) 개수
df.groupBy('Gender').count().show(truncate=False)

# COMMAND ----------

# 열 하나의 합계
df.groupBy('Gender').sum('YearlyIncome').show(truncate=False)

# COMMAND ----------

# 여러 열 기준 집계
df.groupBy(['Gender', 'MaritalStatus']).sum("YearlyIncome").show(truncate=False)
#df.groupBy('Gender', 'MaritalStatus').sum("YearlyIncome").show(truncate=False)

# COMMAND ----------

# 여러 집계
from pyspark.sql.functions import sum, avg, min, max, count, count_distinct

df.groupBy('Gender') \
    .agg(sum('YearlyIncome').alias('sum_salary'), \
         avg('YearlyIncome').alias('avg_salary'), \
         min('YearlyIncome').alias('min_salary'), \
         max('YearlyIncome').alias('max_salary'), \
         count('CustomerKey').alias('customer_cnt'), \
         count_distinct('EnglishOccupation').alias('occupation_cnt') \
         ) \
    .show(truncate=False)


# COMMAND ----------

# 집계 이후 필터
from pyspark.sql.functions import col

df.groupBy('Gender') \
    .agg(sum('YearlyIncome').alias('sum_salary'), \
         avg('YearlyIncome').alias('avg_salary'), \
         min('YearlyIncome').alias('min_salary'), \
         max('YearlyIncome').alias('max_salary'), \
         count('CustomerKey').alias('customer_cnt'), \
         count_distinct('EnglishOccupation').alias('occupation_cnt') \
         ) \
    .filter(col('avg_salary') >= 57300) \
    .show(truncate=False)

# COMMAND ----------

# \ 표시 없음 다음줄 쓰기
df2 = (df.groupBy('Gender') 
    .agg(sum('YearlyIncome').alias('sum_salary'), 
         avg('YearlyIncome').alias('avg_salary'), 
         min('YearlyIncome').alias('min_salary'), 
         max('YearlyIncome').alias('max_salary'), 
         count('CustomerKey').alias('customer_cnt'), 
         count_distinct('EnglishOccupation').alias('occupation_cnt') 
         ) 
    .filter(col('avg_salary') >= 57300) 
)

df2.show()
