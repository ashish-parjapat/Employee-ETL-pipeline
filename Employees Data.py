# Databricks notebook source
dbutils.fs.unmount("/mnt/employees")


# COMMAND ----------

directory_id = "c2bf2940-e699-4738-964b-339474847259" # this is your azure tenant id or azure directory id
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="secret-employees", key="application-id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="secret-employees", key="secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"
}
container_name = "employees"
storage_account_name = "employeesdata2000"
mount_name = "employees"
dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=f"/mnt/{mount_name}",
    extra_configs=configs
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/employees/Bronze/"))



# COMMAND ----------

countries=spark.read.csv("/mnt/employees/Bronze/countries.csv", header=True)
departments=spark.read.csv("/mnt/employees/Bronze/departments.csv", header=True)
employees=spark.read.csv("/mnt/employees/Bronze/employees.csv", header=True)
locations=spark.read.csv("/mnt/employees/Bronze/locations.csv", header=True)

display(employees)

# COMMAND ----------

employees.printSchema()

# COMMAND ----------


from pyspark.sql.types import *
employee_schema =StructType([
  StructField("EMPLOYEE_ID",IntegerType(),False),
  StructField("FIRST_NAME",StringType(),False),
  StructField("LAST_NAME",StringType(),False),
  StructField("EMAIL",StringType(),False),
  StructField("PHONE_NUMBER",StringType(),False),
  StructField("HIRE_DATE",StringType(),False),
  StructField("JOB_ID",StringType(),False),
  StructField("SALARY",IntegerType(),False),
  StructField("MANAGER_ID",IntegerType (),True),
  StructField("DEPARTMENT_ID",IntegerType(),False)
])

employees=spark.read.csv("/mnt/employees/Bronze/employees.csv", header=True, schema=employee_schema)

employees.printSchema()


# COMMAND ----------

from pyspark.sql.functions import to_date, col
employees_silver=employees.select("EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME",  "HIRE_DATE", "JOB_ID", "SALARY", "MANAGER_ID", "DEPARTMENT_ID")

employees_silver=employees_silver.withColumn("HIRE_DATE", to_date(col("HIRE_DATE"), "MM/dd/yyyy").alias("DATE"))

employees_silver.write.format("parquet").mode("overwrite").save("/mnt/employees/Silver/employees.parquet")



# COMMAND ----------

departments.printSchema()

from pyspark.sql.types import *
schema =StructType([
  StructField("DEPARTMENT_ID",IntegerType(),False),
  StructField("DEPARTMENT_NAME",StringType(),False),
  StructField("MANAGER_ID",IntegerType(),False)
  ,StructField("LOCATION_ID",IntegerType(),False)

])

departments=spark.read.csv("/mnt/employees/Bronze/departments.csv", header=True, schema=schema)
departments.printSchema()


# COMMAND ----------

departments_silver=departments.select("DEPARTMENT_ID", "DEPARTMENT_NAME")
departments_silver.write.format("parquet").mode("overwrite").save("/mnt/employees/Silver/departments.parquet")

# COMMAND ----------

countries.printSchema()

from pyspark.sql.types import *
schema =StructType([
  StructField("COUNTRY_ID",StringType(),False),
  StructField("COUNTRY_NAME",StringType(),False)


])

countries=spark.read.csv("/mnt/employees/Bronze/countries.csv", header=True, schema=schema)
countries.printSchema()

# COMMAND ----------

countries_silver=countries.select("COUNTRY_ID", "COUNTRY_NAME")
countries_silver.write.format("parquet").mode("overwrite").save("/mnt/employees/Silver/countries.parquet")


# COMMAND ----------

from pyspark.sql.functions import concat, lit
employees_gold=employees_silver.join(departments_silver, employees_silver.DEPARTMENT_ID==departments_silver.DEPARTMENT_ID, "left").select(employees_silver["EMPLOYEE_ID"],concat(employees_silver["FIRST_NAME"], lit(" "), employees_silver["LAST_NAME"]).alias("FULL_NAME"), employees_silver["HIRE_DATE"], employees_silver["JOB_ID"], employees_silver["SALARY"], departments_silver["DEPARTMENT_NAME"])
display(employees_gold)

employees_gold.write.format("parquet").mode("overwrite").save("/mnt/employees/Gold/employees.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists Employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC use Employees;

# COMMAND ----------

display(dbutils.fs.ls("/mnt/employees/Gold/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Employees.employee(
# MAGIC   EMPLOYEE_ID INT,
# MAGIC   FULL_NAME STRING,
# MAGIC   HIRE_DATE DATE,
# MAGIC   JOB_ID STRING,
# MAGIC   SALARY INT,
# MAGIC   DEPARTMENT_NAME STRING
# MAGIC )
# MAGIC using parquet
# MAGIC location 'dbfs:/mnt/employees/Gold/employees'