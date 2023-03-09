# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.ProductLT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE main.default.ProductLT
# MAGIC (
# MAGIC ProductID INT,
# MAGIC Name STRING,
# MAGIC ProductNumber STRING,
# MAGIC SafetyStockLevel INT,
# MAGIC ReorderPoint INT,
# MAGIC ListPrice STRING,
# MAGIC SellStartDate DATE,
# MAGIC SubcategoryName STRING,
# MAGIC CategoryName STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 헤더가 있는 파일의 로드
# MAGIC COPY INTO main.default.ProductLT
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/ProductLT_Header.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- inferSchema 옵션을 추가하여 헤더가 있는 파일의 로드
# MAGIC COPY INTO main.default.ProductLT
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/ProductLT_Header.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header' = 'true', 'inferSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 헤더가 없는 파일의 내용을 미리 확인해 보자.
# MAGIC SELECT * FROM CSV.`abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/ProductLT_NoHeader.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 헤더가 없는 파일의 로드
# MAGIC COPY INTO main.default.ProductLT
# MAGIC FROM (
# MAGIC   SELECT _c0::INT, _c1::STRING, _c2::STRING, _c3::INT, _c4::INT, _c5::STRING, _c6::DATE, _c7::STRING, _c8::STRING
# MAGIC   FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/ProductLT_NoHeader.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 헤더가 없는 파일의 로드
# MAGIC COPY INTO main.default.ProductLT
# MAGIC FROM (
# MAGIC   SELECT _c0::INT AS ProductID, _c1::STRING AS Name, _c2::STRING AS ProductNumber, _c3::INT AS SafetyStockLevel,
# MAGIC     _c4::INT AS ReorderPoint, _c5::STRING AS ListPrice, _c6::DATE AS SellStartDate, _c7::STRING AS SubcategoryName, _c8::STRING AS CategoryName
# MAGIC   FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/ProductLT_NoHeader.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header' = 'false');
