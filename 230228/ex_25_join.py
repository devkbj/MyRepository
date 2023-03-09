# Databricks notebook source
emp = [(1, "Smith", -1, "2018", 10, "M", 3000), \
       (2, "Rose", 1, "2010", 20, "M", 4000), \
       (3, "Williams", 1, "2010", 10, "M", 1000), \
       (4, "Jones", 2, "2005", 10, "F", 2000), \
       (5, "Brown", 2, "2010", 40, "", -1), \
       (6, "Brown", 2, "2010", 50, "", -1) \
       ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", \
              "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
display(empDF)


# COMMAND ----------

dept = [("Finance", 10), \
        ("Marketing", 20), \
        ("Sales", 30), \
        ("IT", 40) \
        ]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
display(deptDF)


# COMMAND ----------

# 내부(Inner) 조인
df = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'inner')
display(df)

# COMMAND ----------

# left, leftouter
df = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftouter')
display(df)

# COMMAND ----------

# right, rightouter
df = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'rightouter')
display(df)

# COMMAND ----------

# outer, full, fullouter
df = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'fullouter')
display(df)

# COMMAND ----------

# 조인이 안 되는 항목만 not exists..
df = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti")
display(df)


# COMMAND ----------

# 조인 열이 여러개
# 실행 안 된다. 참고만
join_conditions = [empDF.emp_dept1_id == deptDF.dept1_id, empDF.emp_dept2_id == deptDF.dept2_id]
join_conditions = [(empDF.emp_dept1_id == deptDF.dept1_id) | (empDF.emp_dept2_id == deptDF.dept2_id)]
df = empDF.join(deptDF, join_conditions, 'inner')
display(df)
