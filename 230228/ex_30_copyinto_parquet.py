# Databricks notebook source
# MAGIC %md
# MAGIC 준비
# MAGIC 1. dw 폴더의 모든 parquet 파일을 업로드한다.

# COMMAND ----------

# MAGIC %md
# MAGIC # DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unity Catalog에 새로운 테이블 생성한다.
# MAGIC CREATE TABLE main.default.DimCustomer
# MAGIC (
# MAGIC 	CustomerKey INT NOT NULL PRIMARY KEY,
# MAGIC 	GeographyKey INT,
# MAGIC 	CustomerAlternateKey STRING NOT NULL,
# MAGIC 	Title STRING,
# MAGIC 	FirstName STRING,
# MAGIC 	MiddleName STRING,
# MAGIC 	LastName STRING,
# MAGIC 	NameStyle BOOLEAN,
# MAGIC 	BirthDate DATE,
# MAGIC 	MaritalStatus STRING,
# MAGIC 	Suffix STRING,
# MAGIC 	Gender STRING,
# MAGIC 	EmailAddress STRING,
# MAGIC 	YearlyIncome Decimal(19,4),
# MAGIC 	TotalChildren INT,
# MAGIC 	NumberChildrenAtHome INT,
# MAGIC 	EnglishEducation STRING,
# MAGIC 	SpanishEducation STRING,
# MAGIC 	FrenchEducation STRING,
# MAGIC 	EnglishOccupation STRING,
# MAGIC 	SpanishOccupation STRING,
# MAGIC 	FrenchOccupation STRING,
# MAGIC 	HouseOwnerFlag STRING,
# MAGIC 	NumberCarsOwned INT,
# MAGIC 	AddressLine1 STRING,
# MAGIC 	AddressLine2 STRING,
# MAGIC 	Phone STRING,
# MAGIC 	DateFirstPurchase DATE,
# MAGIC 	CommuteDistance STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --  COPY INTO 문을 이용해 dboDimCustomer.parquet 파일을 불러와 DimCustomer 테이블에 입력한다.
# MAGIC COPY INTO main.default.DimCustomer
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 테이블의 내용을 비운다
# MAGIC TRUNCATE TABLE main.default.DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC --  다시 한번 COPY INTO 문을 이용해 dboDimCustomer.parquet 파일을 불러와 DimCustomer 테이블에 입력한다.
# MAGIC COPY INTO main.default.DimCustomer
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC --  force 옵션을 이용해 다시 COPY INTO 문을 이용해 dboDimCustomer.parquet 파일을 불러와 DimCustomer 테이블에 입력한다.
# MAGIC COPY INTO main.default.DimCustomer
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('force' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 테이블을 삭제한다.
# MAGIC DROP TABLE main.default.DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.DimCustomer
# MAGIC (
# MAGIC 	CustomerKey INT NOT NULL PRIMARY KEY,
# MAGIC 	GeographyKey STRING, -- 데이터 형식 변경
# MAGIC 	CustomerAlternateKey STRING NOT NULL,
# MAGIC 	Title STRING,
# MAGIC 	FirstName STRING,
# MAGIC 	MiddleName STRING,
# MAGIC 	LastName STRING,
# MAGIC 	NameStyle BOOLEAN,
# MAGIC 	BirthDate DATE,
# MAGIC 	MaritalStatus STRING,
# MAGIC 	Suffix STRING,
# MAGIC 	Gender STRING,
# MAGIC 	EmailAddress STRING,
# MAGIC 	YearlyIncome Decimal(19,4),
# MAGIC 	TotalChildren INT,
# MAGIC 	NumberChildrenAtHome INT,
# MAGIC 	EnglishEducation STRING,
# MAGIC 	SpanishEducation STRING,
# MAGIC 	FrenchEducation STRING,
# MAGIC 	EnglishOccupation STRING,
# MAGIC 	SpanishOccupation STRING,
# MAGIC 	FrenchOccupation STRING,
# MAGIC 	HouseOwnerFlag STRING,
# MAGIC 	NumberCarsOwned INT,
# MAGIC 	AddressLine1 STRING,
# MAGIC 	AddressLine2 STRING,
# MAGIC 	Phone STRING,
# MAGIC 	DateFirstPurchase DATE,
# MAGIC 	CommuteDistance STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --  COPY INTO 문을 이용해 dboDimCustomer.parquet 파일을 불러와 DimCustomer 테이블에 입력한다.
# MAGIC COPY INTO main.default.DimCustomer
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 문제가 되는 열의 데이터 형식을 변환한다.
# MAGIC COPY INTO main.default.DimCustomer
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT CustomerKey, GeographyKey::STRING, CustomerAlternateKey, Title, 
# MAGIC       FirstName, MiddleName, LastName, NameStyle, BirthDate, 
# MAGIC       MaritalStatus, Suffix, Gender, EmailAddress, YearlyIncome, TotalChildren, 
# MAGIC       NumberChildrenAtHome, EnglishEducation, SpanishEducation, FrenchEducation, 
# MAGIC       EnglishOccupation, SpanishOccupation, FrenchOccupation, HouseOwnerFlag, 
# MAGIC       NumberCarsOwned, AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance
# MAGIC     FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC )
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %md
# MAGIC # Employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE main.default.employee
# MAGIC (
# MAGIC emp_id INT,
# MAGIC name STRING,
# MAGIC superior_emp_id INT,
# MAGIC year_joined INT,
# MAGIC emp_dept_id INT,
# MAGIC gender STRING,
# MAGIC salary INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 올바른 데이터 파일을 로드한다.
# MAGIC COPY INTO main.default.employee
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/employees/employee_1_origin.parquet'
# MAGIC FILEFORMAT = PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 입력된 데이터를 확인한다.
# MAGIC SELECT * FROM main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 열이 추가된 파일을 로드한다.
# MAGIC COPY INTO main.default.employee
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/employees/employee_3_addcolumn.parquet'
# MAGIC FILEFORMAT = PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mergeSchema 옵션을 이용해 열이 추가된 파일을 로드한다.
# MAGIC COPY INTO main.default.employee
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/employees/employee_3_addcolumn.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE main.default.employee
# MAGIC (
# MAGIC emp_id INT,
# MAGIC name STRING,
# MAGIC superior_emp_id INT,
# MAGIC year_joined INT,
# MAGIC emp_dept_id INT,
# MAGIC gender STRING,
# MAGIC salary INT,
# MAGIC email STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 디렉토리의 모든 파일을 로드한다.
# MAGIC COPY INTO main.default.employee
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/employees'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE main.default.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 디렉토리의 모든 파일을 로드한다.
# MAGIC COPY INTO main.default.employee
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/employees'
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('force' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.employee
