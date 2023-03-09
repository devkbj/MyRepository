# Databricks notebook source
storage_account = 'mcbedu11'
file_path = 'abfss://adls@{}.dfs.core.windows.net/datalake/dboDimCustomer.parquet'.format(storage_account)
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

# YearlyIncome 열의 값에 1230을 곱하기
from pyspark.sql.functions import col

df3 = df.withColumn("YearlyIncome_Kor", col("YearlyIncome") * 1230)
df3.printSchema()
display(df3)

# COMMAND ----------

# USA라는 고정 값을 가진 열 추가
from pyspark.sql.functions import lit
df4 = df.withColumn('Country', lit('USA'))
df4.printSchema()
display(df4)


# COMMAND ----------

# 열 이름 바꾸기
df.withColumnRenamed('Title', 'JobPosition').show(truncate=False)

# COMMAND ----------

# 소문자로 바꾸기
from pyspark.sql.functions import lower
df5 = df.withColumn('FirstNameL', lower('FirstName'))\
    .filter(col('FirstNameL').startswith('ka'))\
    .drop(col('FirstNameL'))

display(df5)    


# COMMAND ----------

# 문자열 이어 붙이기
# 공백은 lit(' ')으로 표시해야 한다. ' '이라고만 적으면 스페이스 하나가 이름인 열을 찾는다.
# null + 문자 = null 문제는 사용자 정의 함수로 해결해보자.

from pyspark.sql.functions import concat, year, current_date
df5 = df.withColumn('Fullname', concat('FirstName', lit(' '), 'MiddleName', lit(' '), 'LastName')) \
    .withColumn('BirthYear', year('BirthDate')) \
    .withColumn('Today', current_date()) \
    .withColumn('Age', year(col('Today')) - col('BirthYear'))
display(df5)

# COMMAND ----------

# 단일 열 반환 UDF

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def make_fullname_(fname, mname, lname):
    if mname:
        return '{} {} {}'.format(fname, mname, lname)
    else:
        return '{} {}'.format(fname, lname)

make_fullname = udf(make_fullname_, StringType())
df6 = df\
    .withColumn('FullName', make_fullname('FirstName', 'MiddleName', 'LastName'))

display(df6)

# COMMAND ----------

# 여러 열 반환 UDF
# 이메일 주소에서 아이디와 호스팅을 분리해보자.
from pyspark.sql.types import StructType, StructField, StringType

def split_email_(email):
    e = email.split('@')
    return e[0], e[1]

email_schema = StructType([
    StructField('id', StringType(), True),
    StructField('hosting', StringType(), True)
])

split_email = udf(split_email_, email_schema)

df7 = df\
    .withColumn('Email', split_email('EmailAddress'))

display(df7)
# display(df7.select('Email.id', 'Email.hosting'))
