# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.SourceProducts
# MAGIC (
# MAGIC     ProductID		INT,
# MAGIC     ProductName		VARCHAR(50),
# MAGIC     Price			DECIMAL(9,2)
# MAGIC );
# MAGIC     
# MAGIC INSERT INTO main.default.SourceProducts(ProductID,ProductName, Price) 
# MAGIC VALUES(1,'Table',100), (2,'Desk',80), (3,'Chair',50), (4,'Computer',300);   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.TargetProducts
# MAGIC (
# MAGIC     ProductID		INT,
# MAGIC     ProductName		VARCHAR(50),
# MAGIC     Price			DECIMAL(9,2)
# MAGIC );
# MAGIC     
# MAGIC INSERT INTO main.default.TargetProducts(ProductID,ProductName, Price) 
# MAGIC VALUES (1,'Table',100), (2,'Desk',180), (5,'Bed',50), (6,'Cupboard',300);   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.SourceProducts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.TargetProducts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 런타임 버전 12.1 이상
# MAGIC -- MATCHED, NOT MATCHED BY TARGET, NOT MATCHED BY SOURCE 순서로 적어야 한다.
# MAGIC MERGE INTO main.default.TargetProducts AS Target
# MAGIC USING main.default.SourceProducts AS Source ON Source.ProductID = Target.ProductID
# MAGIC WHEN MATCHED AND Target.Price <> Source.Price THEN 
# MAGIC   UPDATE SET
# MAGIC       Target.ProductName = Source.ProductName,
# MAGIC       Target.Price		 = Source.Price
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC   INSERT (ProductID, ProductName, Price) 
# MAGIC   VALUES (Source.ProductID,Source.ProductName, Source.Price)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.TargetProducts

# COMMAND ----------

product = [(1, 'Table', 100.0), (2, 'Desk', 80.0), (3, 'Chair', 50.0), (4, 'Computer', 300.0)]
productColumns = ['ProductID' ,'ProductName' , 'Price']

source_df = spark.createDataFrame(data=product, schema=productColumns)
source_df.createTempView('SrcProduct')
source_df.printSchema()
display(source_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 런타임 버전 12.1 이상
# MAGIC -- MATCHED, NOT MATCHED BY TARGET, NOT MATCHED BY SOURCE 순서로 적어야 한다.
# MAGIC MERGE INTO main.default.TargetProducts AS Target
# MAGIC USING SrcProduct AS Source ON Source.ProductID = Target.ProductID
# MAGIC WHEN MATCHED AND Target.Price <> Source.Price THEN 
# MAGIC   UPDATE SET
# MAGIC       Target.ProductName = Source.ProductName,
# MAGIC       Target.Price		 = Source.Price
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC   INSERT (ProductID, ProductName, Price) 
# MAGIC   VALUES (Source.ProductID,Source.ProductName, Source.Price)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;
# MAGIC     
