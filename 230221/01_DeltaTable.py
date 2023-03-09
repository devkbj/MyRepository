# Databricks notebook source
# 기본 카탈로그를 지정한다
spark.sql('USE CATALOG main')

# COMMAND ----------

# 자신의 계정명으로된 스키마를 생성한다.
username = spark.sql('SELECT current_user()').first()[0]
username = username.split('@')[0]

sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(username)
spark.sql(sql)

# COMMAND ----------

# 기본 스키마를 지정한다.
sql = 'USE SCHEMA {}'.format(username)
spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unity Catalog에 새로운 테이블 생성한다.
# MAGIC CREATE OR REPLACE TABLE DimCustomer
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
# MAGIC COPY INTO DimCustomer
# MAGIC FROM 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboDimCustomer.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC --  SELECT 문을 이용해 DimCustomer 테이블의 일부 내용을 조회한다.
# MAGIC SELECT * FROM DimCustomer LIMIT 10;

# COMMAND ----------

# 파이썬을 이용해 DimCustomer 테이블의 데이터를 Dataframe 형태로 불러와 display 함수로 출력한다.
df = spark.read.table('DimCustomer')
display(df)

# COMMAND ----------

# 제품과 관련된 3개의 테이블을 조인하고 그 결과를 화면에 출력한다.
file_name = 'abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/{}.parquet'
df_product_main = spark.read.load(file_name.format('dboDimProduct'), format='parquet')
df_product_subcategory = spark.read.load(file_name.format('dboDimProductSubcategory'), format='parquet')
df_product_category = spark.read.load(file_name.format('dboDimProductCategory'), format='parquet')

df_product = df_product_main \
    .join(df_product_subcategory,
          df_product_main.ProductSubcategoryKey == df_product_subcategory.ProductSubcategoryKey, 'left') \
    .join(df_product_category,
          df_product_subcategory.ProductCategoryKey == df_product_category.ProductCategoryKey, 'left') \
    .select('ProductKey', 'EnglishProductName', 'ListPrice', 'ModelName', 'EnglishProductSubcategoryName', 'EnglishProductCategoryName')

display(df_product)

# COMMAND ----------

# 앞에서 작업한 데이터 프레임을 델타 테이블로 저장한다.
df_product.write.format('delta').mode('overwrite').saveAsTable('DimProduct')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT 문을 이용해 DimCustomer 테이블의 일부 내용을 조회한다.
# MAGIC SELECT * FROM DimProduct LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FactInternetSales 테이블을 생성한다.
# MAGIC CREATE OR REPLACE TABLE FactInternetSales
# MAGIC (
# MAGIC 	ProductKey INT NOT NULL,
# MAGIC 	OrderDateKey INT NOT NULL,
# MAGIC 	DueDateKey INT NOT NULL,
# MAGIC 	ShipDateKey INT NOT NULL,
# MAGIC 	CustomerKey INT NOT NULL REFERENCES DimCustomer (CustomerKey),
# MAGIC 	PromotionKey INT NOT NULL,
# MAGIC 	CurrencyKey INT NOT NULL,
# MAGIC 	SalesTerritoryKey INT NOT NULL,
# MAGIC 	SalesOrderNumber STRING NOT NULL,
# MAGIC 	SalesOrderLineNumber INT NOT NULL,
# MAGIC 	RevisionNumber INT NOT NULL,
# MAGIC 	OrderQuantity INT NOT NULL,
# MAGIC 	UnitPrice INT NOT NULL,
# MAGIC 	ExtendedAmount INT NOT NULL,
# MAGIC 	UnitPriceDiscountPct FLOAT NOT NULL,
# MAGIC 	DiscountAmount FLOAT NOT NULL,
# MAGIC 	ProductStandardCost INT NOT NULL,
# MAGIC 	TotalProductCost INT NOT NULL,
# MAGIC 	SalesAmount INT NOT NULL,
# MAGIC 	TaxAmt INT NOT NULL,
# MAGIC 	Freight INT NOT NULL,
# MAGIC 	CarrierTrackingNumber STRING,
# MAGIC 	CustomerPONumber STRING,
# MAGIC 	OrderDate TIMESTAMP,
# MAGIC 	DueDate TIMESTAMP,
# MAGIC 	ShipDate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FactInternetSales 테이블에 데이터를 입력하기 위해 INSERT INTO 문을 실행한다.
# MAGIC -- 이를 위해서 SELECT 문을 이용해 외부 파일을 불러온다.
# MAGIC INSERT INTO FactInternetSales
# MAGIC SELECT *
# MAGIC FROM PARQUET.`abfss://adls@mcloudbridge8.dfs.core.windows.net/datalake/dboFactInternetSales.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2개의 테이블을 조인하여 데이터를 집계하여 출력해본다.
# MAGIC SELECT c.Gender, SUM(s.SalesAmount)
# MAGIC FROM DimCustomer AS c
# MAGIC   JOIN FactInternetSales AS s ON c.CustomerKey = s.CustomerKey
# MAGIC GROUP BY c.Gender

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DimProduct 테이블에서 상품코드가 528, 607인 상품을 조회하는 내용이다.
# MAGIC -- 결과를 살펴보면 607번 상품은 아직 입력되지 않았고, 528번 상품의 가격(ListPrice)는 4.99임을 기억해 두자.
# MAGIC -- 뒤에서 해당 테이블의 내용을 갱신하고 다시 결과를 살펴볼 것이다.
# MAGIC SELECT  *
# MAGIC FROM DimProduct
# MAGIC WHERE ProductKey in (528, 607)

# COMMAND ----------

# 업데이트할 제품 정보를 담고있는 dboDimProductUpdate.parquet 파일을 불러와 StgProduct라는 이름의 테이블 뷰를 생성한다.
df_update = spark.read.load(file_name.format('dboDimProductUpdate'), format='parquet')
df_update.createTempView('StgProduct')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE 문을 이용해 DimProduct 테이블의 내용을 추가, 변경 작업을 수행한다.
# MAGIC -- 이때 앞에서 만든 변경할 내용을 포함한 StgProduct 테이블 뷰를 이용한다.
# MAGIC MERGE INTO DimProduct
# MAGIC USING StgProduct
# MAGIC   ON DimProduct.ProductKey = StgProduct.ProductKey
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 이제 528, 607번 상품이 모두 조회가 된다.
# MAGIC -- 그리고 결과를 살펴보면 528번 상품의 가격(ListPrice)이 바뀐것을 알 수 있다.
# MAGIC SELECT  *
# MAGIC FROM DimProduct
# MAGIC WHERE ProductKey in (528, 607)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 테이블 이름과 함께 버전을 기입하면 과거 데이터를 볼 수 있다.
# MAGIC -- 여기서 VERSION AS OF 0은 테이블의 첫 번째 버전, 즉 최초 데이터가 입력된 시점의 데이터를 보여준다.
# MAGIC -- 그래서 아래 결과를 살펴보면 607번 상품이 조회되지 않는다.
# MAGIC SELECT *
# MAGIC FROM DimProduct VERSION AS OF 0
# MAGIC WHERE ProductKey in (528, 607)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY 문을 이용해 DimProduct 테이블의 변경 이력을 조회해보자.
# MAGIC DESCRIBE HISTORY DimProduct
