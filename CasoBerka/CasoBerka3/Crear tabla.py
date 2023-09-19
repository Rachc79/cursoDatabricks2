# Databricks notebook source
# MAGIC %md
# MAGIC ## Creacion de tabla para regresión

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructType, StructField
 
schema = StructType([
  StructField("frequency", StringType(), True),
  StructField("date1", DoubleType(), True),
  StructField("date", DoubleType(), True),
  StructField("amount", DoubleType(), True),
  StructField("duration", DoubleType(), True),
  StructField("payments", DoubleType(), True),
  StructField("status", StringType(), True),
  StructField("cliend_id", DoubleType(), True),
  StructField("birth_number", DoubleType(), True),
  StructField("churn", DoubleType(), True),
])
 
from pyspark.sql import SparkSession

# Crea una sesión Spark
spark = SparkSession.builder.appName("Ejemplo").getOrCreate()

# Carga los datos desde el archivo Parquet
housing_df = spark.read.parquet("dbfs:/mnt/your-data-lake/tabla_minable")

# Ahora, puedes trabajar con el DataFrame housing_df 
# housing_df = spark.read.format("csv").schema(schema).option("header", "true").load("/FileStore/housing.csv")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS default.housing_t")

# COMMAND ----------

#housing_df.write.saveAsTable("default.housing_t")
#housing_df.write.mode("overwrite").saveAsTable("default.housing_t")
housing_df.write.saveAsTable("default.housing_t")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from housing_t

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creacion de tabla para clasificación

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructType, StructField
 
schema = StructType([
  StructField("frequency", StringType(), True),
  StructField("date1", DoubleType(), True),
  StructField("date", DoubleType(), True),
  StructField("amount", DoubleType(), True),
  StructField("duration", DoubleType(), True),
  StructField("payments", DoubleType(), True),
  StructField("status", StringType(), True),
  StructField("cliend_id", DoubleType(), True),
  StructField("birth_number", DoubleType(), True),
  StructField("churn", DoubleType(), True),
])
 
from pyspark.sql import SparkSession

# Crea una sesión Spark
spark = SparkSession.builder.appName("Ejemplo").getOrCreate()

# Carga los datos desde el archivo Parquet
census_df = spark.read.parquet("dbfs:/mnt/your-data-lake/tabla_minable")
#census_df = spark.read.format("csv").schema(schema).load("/databricks-datasets/adult/adult.data")


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS default.census_t")

# COMMAND ----------

census_df.write.saveAsTable("default.census_t")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM census_t
