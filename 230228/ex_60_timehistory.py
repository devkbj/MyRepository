# Databricks notebook source
# 제품과 관련된 3개의 테이블을 조인하고 그 결과를 화면에 출력한다.
account = 'mcloudbridge8'
directory = f'abfss://adls@{account}.dfs.core.windows.net/datalake'

df_product_main = spark.read.parquet('{}/{}.parquet'.format(directory, 'dboDimProduct'))
df_product_subcategory = spark.read.parquet('{}/{}.parquet'.format(directory, 'dboDimProductSubcategory'))
df_product_category = spark.read.parquet('{}/{}.parquet'.format(directory, 'dboDimProductCategory'))

df_product = df_product_main \
    .join(df_product_subcategory,
          df_product_main.ProductSubcategoryKey == df_product_subcategory.ProductSubcategoryKey, 'left') \
    .join(df_product_category,
          df_product_subcategory.ProductCategoryKey == df_product_category.ProductCategoryKey, 'left') \
    .select('ProductKey', 'EnglishProductName', 'ListPrice', 'ModelName', 'EnglishProductSubcategoryName', 'EnglishProductCategoryName')

display(df_product)

# COMMAND ----------

# 앞에서 작업한 데이터 프레임을 델타 테이블로 저장한다.
spark.sql('DROP TABLE IF EXISTS main.default.DimProduct')
df_product.write.format('delta').mode('overwrite').saveAsTable('main.default.DimProduct')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT 문을 이용해 DimCustomer 테이블의 일부 내용을 조회한다.
# MAGIC SELECT * FROM main.default.DimProduct LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DimProduct 테이블에서 상품코드가 528, 607인 상품을 조회하는 내용이다.
# MAGIC -- 결과를 살펴보면 607번 상품은 아직 입력되지 않았고, 528번 상품의 가격(ListPrice)는 4.99임을 기억해 두자.
# MAGIC -- 뒤에서 해당 테이블의 내용을 갱신하고 다시 결과를 살펴볼 것이다.
# MAGIC SELECT  *
# MAGIC FROM main.default.DimProduct
# MAGIC WHERE ProductKey in (528, 607)

# COMMAND ----------

# 업데이트할 제품 정보를 담고있는 dboDimProductUpdate.parquet 파일을 불러와 StgProduct라는 이름의 테이블 뷰를 생성한다.
df_update = spark.read.parquet('{}/{}.parquet'.format(directory, 'dboDimProductUpdate'))
df_update.createTempView('StgProduct')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA default;

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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 테이블을 이전 상태로 복원한다.
# MAGIC RESTORE TABLE DimProduct TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 다시 데이터를 조회해보자
# MAGIC SELECT *
# MAGIC FROM DimProduct
# MAGIC WHERE ProductKey in (528, 607)
