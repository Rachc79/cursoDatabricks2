-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW account1
USING com.databricks.spark.csv
OPTIONS (
  path "dbfs:/FileStore/tables/account.asc",
  header "true",  
  delimiter ";"  
);

-- COMMAND ----------

select *
from account1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW loan1
USING com.databricks.spark.csv
OPTIONS (
  path "dbfs:/FileStore/tables/loan.asc",
  header "true",  
  delimiter ";"  
);

-- COMMAND ----------

select *
from loan1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW client1
USING com.databricks.spark.csv
OPTIONS (
  path "dbfs:/FileStore/tables/client.asc",
  header "true",  
  delimiter ";"  
);


-- COMMAND ----------

select *
from client1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Crea una sesión de Spark
-- MAGIC spark = SparkSession.builder.appName("TablaMinable1").getOrCreate()
-- MAGIC
-- MAGIC # Lee las tablas desde sus ubicaciones o fuentes de datos
-- MAGIC account1 = spark.read.option("delimiter", ";").csv("dbfs:/FileStore/tables/account.asc", header=True, inferSchema=True)
-- MAGIC loan1 = spark.read.option("delimiter", ";").csv("dbfs:/FileStore/tables/loan.asc", header=True, inferSchema=True)
-- MAGIC client1 = spark.read.option("delimiter", ";").csv("dbfs:/FileStore/tables/client.asc", header=True, inferSchema=True)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ver las columnas de la tabla account1
-- MAGIC print("Columnas de account1:")
-- MAGIC print(account1.columns)
-- MAGIC
-- MAGIC # Ver las columnas de la tabla loan1
-- MAGIC print("\nColumnas de loan1:")
-- MAGIC print(loan1.columns)
-- MAGIC
-- MAGIC # Ver las columnas de la tabla client1
-- MAGIC print("\nColumnas de client1:")
-- MAGIC print(client1.columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Crear una sesión de Spark
-- MAGIC # spark = SparkSession.builder.appName("Ejemplo").getOrCreate()
-- MAGIC
-- MAGIC # Supongamos que ya tienes el DataFrame account1
-- MAGIC # Cambiar el nombre de la columna "date" a "date1"
-- MAGIC account1 = account1.withColumnRenamed("date", "date1")
-- MAGIC
-- MAGIC # Verificar el cambio de nombre
-- MAGIC account1.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Realiza las uniones utilizando las columnas adecuadas
-- MAGIC # Primero, une account1 y loan1 en 'account_id'
-- MAGIC tabla_intermedia = account1.join(loan1, 'account_id', 'inner')
-- MAGIC
-- MAGIC # Luego, une la tabla intermedia con client1 en 'district_id'
-- MAGIC tabla_minable = tabla_intermedia.join(client1, 'district_id', 'inner')
-- MAGIC
-- MAGIC # Guarda la tabla minable en una ubicación deseada
-- MAGIC tabla_minable.write.csv("dbfs:/FileStore/tables/tabla_minable", header=True, mode="overwrite")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Guarda la tabla minable en una ubicación deseada en formato CSV
-- MAGIC tabla_minable.write.csv("dbfs:/FileStore/tables/tabla_minable", header=True, mode="overwrite")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Crear la nueva columna "churn" basada en la columna "status"
-- MAGIC tabla_minable = tabla_minable.withColumn(
-- MAGIC     "churn",
-- MAGIC     F.when((tabla_minable["status"] == "A") | (tabla_minable["status"] == "C"), 1).otherwise(0)
-- MAGIC )
-- MAGIC
-- MAGIC # Muestra el DataFrame resultante
-- MAGIC tabla_minable.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Lista de columnas que deseas eliminar
-- MAGIC columnas_a_eliminar = ['district_id', 'account_id', 'loan_id']
-- MAGIC
-- MAGIC # Elimina las columnas especificadas
-- MAGIC tabla_minable = tabla_minable.drop(*columnas_a_eliminar)
-- MAGIC tabla_minable.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Especifica la ruta de destino donde deseas guardar la tabla después de eliminar las columnas
-- MAGIC ruta_destino_despues_eliminar = "dbfs:/mnt/your-data-lake/tabla_minable"
-- MAGIC
-- MAGIC # Guarda el DataFrame en formato Parquet (puedes cambiar el formato si lo deseas)
-- MAGIC tabla_minable.write.mode("overwrite").parquet(ruta_destino_despues_eliminar)
-- MAGIC
-- MAGIC # Imprime la dirección de la tabla guardada
-- MAGIC print("La tabla se ha guardado en la siguiente dirección después de eliminar las columnas:", ruta_destino_despues_eliminar)
