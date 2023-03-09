# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. access_log_Aug95_Limit.log 파일 업로드

# COMMAND ----------

# 파일 목록을 확인
account = 'mcloudbridge8'
directory = 'abfss://adls@{}.dfs.core.windows.net/datalake'.format(account)
dbutils.fs.ls(directory)

# COMMAND ----------

# 스키마 정의
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType    

schema = StructType([
StructField('host',StringType()),
StructField('u1',StringType()),
StructField('u2',StringType()),
StructField('date',StringType()),
StructField('time', StringType()),
StructField('access', StringType()),
StructField('status', IntegerType()),
StructField('size', IntegerType())
])


# COMMAND ----------

# 파일 로드
file_path = '{}/access_log_Aug95_Limit.log'.format(directory)
df = spark.read.format('csv')\
    .option('delimiter', ' ')\
    .option('header', 'false')\
    .option('mode', 'DROPMALFORMED')\
    .schema(schema)\
    .load(file_path)

display(df)

# COMMAND ----------

from pyspark.sql.functions import udf
from datetime import timezone, timedelta, datetime

def get_access_datetime_(dt, tz):
    timezone_pm = tz[0]
    if timezone_pm == '-':
        timezone_hour = int(tz[1:3]) * -1
        timezone_minute = int(tz[3:-1]) * -1
    else:
        timezone_hour = int(tz[1:3])
        timezone_minute = int(tz[3:-1])

    timezone_tw = timezone(timedelta(hours=timezone_hour, minutes=timezone_minute))
    date = dt[1:]
    date = datetime.strptime(date, '%d/%b/%Y:%H:%M:%S').replace(tzinfo=timezone_tw)
    
    return date

# COMMAND ----------

get_access_datetime = udf(get_access_datetime_, TimestampType())
df2 = df\
    .withColumn('access_time', get_access_datetime('date', 'time'))\
    .drop('u1', 'u2', 'date', 'time')

display(df2)

# COMMAND ----------

def split_access_(s):
    access_split = s.split(' ')
    return access_split[0], access_split[1], access_split[2]

schema_access = StructType([
	StructField('method', StringType()),
	StructField('uri', StringType()),
	StructField('protocol', StringType())
])

split_access = udf(split_access_, schema_access)

df3 = df2\
    .withColumn('access_method', split_access('access'))\
    .drop('access')\
    .select('host', 'access_time', 'access_method.*' ,'status', 'size')
  
display(df3)
