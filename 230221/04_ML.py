# Databricks notebook source
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble
import mlflow
import mlflow.sklearn

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup
from mlflow.models.signature import infer_signature
from pyspark.sql.functions import monotonically_increasing_id, expr, rand, lit, col, when

# COMMAND ----------

# 데이터 로드 및 전처리
white_wine = spark.read.load('/databricks-datasets/wine-quality/winequality-white.csv',
							 format='csv',sep=';',inferSchema='true',header='true')
red_wine = spark.read.load('/databricks-datasets/wine-quality/winequality-red.csv',
						   format='csv',sep=';',inferSchema='true',header='true')

white_wine = white_wine.withColumn('is_red', lit(0))
red_wine = red_wine.withColumn('is_red', lit(1))
data_df = white_wine.unionAll(red_wine)

# COMMAND ----------

def addIdColumn(dataframe, id_column_name):
	'''데이터 프레임에 id열을 추가한다'''
	columns = dataframe.columns
	new_df = dataframe.withColumn(id_column_name, monotonically_increasing_id())
	return new_df[[id_column_name] + columns]

def renameColumns(df):
	'''기능 저장소와 호환 가능하도록 열 이름을 바꾼다'''
	renamed_df = df
	for column in df.columns:
		renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
	return renamed_df

# Run functions
renamed_df = renameColumns(data_df)
df = addIdColumn(renamed_df, 'wine_id')

# 기능(features) 데이터에서 wine_id, alcohol 열을 제거한다.
features_df = df.drop('wine_id').drop('alcohol')

# COMMAND ----------

# 훈련 데이터세트와 테스트 데이터세트를 생성한다.
training_pd = features_df.toPandas()
X = training_pd.drop('quality', axis=1)
y = training_pd['quality']
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

username = spark.sql('SELECT current_user()').first()[0]
username = username.split('@')[0]
model_name = f'{username}_wine_model'

# MLFlow 모델이 이미 존재하고 있다면 삭제한다.
try:
	client.delete_registered_model(model_name)  
except:
	None

# COMMAND ----------

#  MLflow 자동 로깅을 비활성화한다.
mlflow.sklearn.autolog(log_models=False)

# COMMAND ----------

def train_model(run_name, model, X_train, X_test, y_train, y_test, model_name):
	## fit and log model
	with mlflow.start_run(run_name=run_name) as run:
		model.fit(X_train, y_train)

		y_pred = model.predict(X_test)
		mlflow.log_metric("test_mse", sklearn.metrics.mean_squared_error(y_test, y_pred))
		mlflow.log_metric("test_r2_score", sklearn.metrics.r2_score(y_test, y_pred))
        
		print("test_mse", sklearn.metrics.mean_squared_error(y_test, y_pred))
		print("test_r2_score", sklearn.metrics.r2_score(y_test, y_pred))        

        # MLFlow 모델에 입력값과 출력값을 저장하기 위해 시그니처를 추가한다.
		signature = infer_signature(X_train, model.predict(X_train))
		mlflow.sklearn.log_model(model, model_name, signature=signature)

		# MLFlow 모델을 등록한다.
		run_id = mlflow.active_run().info.run_id
		model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=model_name)
		mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# 첫번째 모델을 트레이닝한다.
model1 = sklearn.ensemble.RandomForestRegressor(random_state=42)
train_model('random_forest_1', model1, X_train, X_test, y_train, y_test, model_name)

# COMMAND ----------

# 두번째 모델은 파라미터를 바꿔 트레이닝한다.
model2 = sklearn.ensemble.RandomForestRegressor(n_estimators=200, random_state=42)
train_model('random_forest_2', model2, X_train, X_test, y_train, y_test, model_name)

# COMMAND ----------

# 기능 데이터에 alcohol 열을 추가하기 위해 df에서 wine_id 열만 제거한다.
features_df = df.drop('wine_id')

training_pd = features_df.toPandas()
X = training_pd.drop('quality', axis=1)
y = training_pd['quality']
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

# alcohol 열이 추가된 모델을 트레이닝한다.
model3 = sklearn.ensemble.RandomForestRegressor(n_estimators=200, random_state=42)
train_model('random_forest_3', model3, X_train, X_test, y_train, y_test, model_name)

# COMMAND ----------

# MLFlow 모델에 등록된 모델을 불러와 사용할 수 있다.
rf_model = mlflow.sklearn.load_model(f"models:/{model_name}/latest")
preds = rf_model.predict(X_test)
preds

# COMMAND ----------

# 지금까지 작업한 기능 데이터를 기능 저장소에 저장한다.
fs = feature_store.FeatureStoreClient()
table_name = f'wine_db.{username}_wine_table'
features_df = df.drop('quality')

fs.create_table(
    name=table_name,
    primary_keys=['wine_id'],
    df=features_df,
    schema=features_df.schema,
    description='wine features'
)

# COMMAND ----------

# 이제 기능 저장소에 저장된 기능 데이터를 불러와 트레이닝 작업을 수행해보자.
# 기능 저장소에는 라벨(quality)값이 없기 때문에 inference_data_df 라는 이름의 라벨값을 포함하는 데이터세트를 생성한다.
inference_data_df = df.select('wine_id', 'quality')
display(inference_data_df)

# COMMAND ----------

def load_data(table_name, lookup_key, inference_data_df):
	# FeatureLookup 함수에서 feature_names 파라미터의 값을 제공하지 key를 제외한 모든 열이 반환된다.
	model_feature_lookups = [FeatureLookup(table_name=table_name, lookup_key=lookup_key)]

	# fs.create_training_set에서는 inference_data_df와  model_feature_lookups에서 키 값이 동일한 행들을 매칭하여 반환해준다.
	training_set = fs.create_training_set(inference_data_df,
										  model_feature_lookups,
										  label='quality',
										  exclude_columns='wine_id')
	training_pd = training_set.load_df().toPandas()

	# 훈련 데이터세트와 테스트 데이터세트를 생성한다.
	X = training_pd.drop('quality', axis=1)
	y = training_pd['quality']
	X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, y, test_size=0.2, random_state=42)
	return X_train, X_test, y_train, y_test, training_set


# 훈련 데이터세트와 테스트 데이터세트를 생성한다.
X_train, X_test, y_train, y_test, training_set = load_data(table_name, 'wine_id', inference_data_df)
X_train.head()

# COMMAND ----------

model4 = sklearn.ensemble.RandomForestRegressor(max_depth=3, n_estimators=200, random_state=42)
train_model('random_forest_4', model3, X_train, X_test, y_train, y_test, model_name)
