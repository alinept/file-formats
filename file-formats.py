# Databricks notebook source
# MAGIC %md
# MAGIC ### JSON format

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/couse_files/")
dbutils.fs.mkdirs("/FileStore/tables/course_files/")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/course_files/"))

# COMMAND ----------

df = spark.read.json("/FileStore/tables/course_files/PNSB.json",multiLine=True)

display(df)

# COMMAND ----------



# COMMAND ----------

df = df.withColumnRenamed("D1C","cod_regiao") \
            .withColumnRenamed("D1N", "regiao")\
            .withColumnRenamed("D2C","cod_variavel") \
            .withColumnRenamed("D2N", "variavel") \
            .withColumnRenamed("D3C", "cod_ano") \
            .withColumnRenamed("D3N", "ano") \
            .withColumnRenamed("D4C","cod_doenca") \
            .withColumnRenamed("D4N", "doenca") \
            .withColumnRenamed("MC","cod_medida") \
            .withColumnRenamed("MN", "medida") \
            .withColumnRenamed("NC","cod_nivel_territorial") \
            .withColumnRenamed("NN", "nivel territorial") \
            .withColumnRenamed("V","valor") 

display (df)


# COMMAND ----------

df = df.filter(df.valor != 'Valor')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

df_new = df.withColumn("cod_regiao", col("cod_regiao").cast(IntegerType())) \
        .withColumn("cod_variavel", col("cod_variavel").cast(IntegerType())) \
        .withColumn("cod_ano", col("cod_ano").cast(IntegerType())) \
        .withColumn("ano", col("ano").cast(IntegerType())) \
        .withColumn("cod_doenca", col("cod_doenca").cast(IntegerType())) \
        .withColumn("cod_medida", col("cod_medida").cast(IntegerType())) \
        .withColumn("cod_nivel_territorial", col("cod_nivel_territorial").cast(IntegerType())) \
        .withColumn("valor", col("valor").cast(IntegerType()))


# COMMAND ----------

df_new.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/course_files/"))

# COMMAND ----------

df_new.write \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .format("json") \
        .save("/FileStore/tables/course_files/json_gzip/")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/course_files/json_gzip"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading a compressed file

# COMMAND ----------

df = spark.read \
          .option("compression", "gzip") \
          .json("/FileStore/tables/course_files/json_gzip/")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving data as CSV
# MAGIC

# COMMAND ----------

df.write \
  .option("sep", ",") \
  .option("header", True) \
  .mode("overwrite") \
  .format("csv") \
  .save("/FileStore/tables/course_files/pnsb_csv/")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/course_files/pnsb_csv/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading a CSV file

# COMMAND ----------

df_csv = spark.read.csv('/FileStore/tables/course_files/pnsb_csv/', header=True, inferSchema=True)
display(df_csv)

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compressing a CVS file 
# MAGIC

# COMMAND ----------

df_csv.write \
      .option("compression", "gzip") \
      .option("header", "true") \
      .option("sep", ",") \
      .mode("overwrite") \
      .format("csv") \
      .save("/FileStore/tables/course_files/csv_gzip/") 

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/course_files/csv_gzip/"))

# COMMAND ----------

df = spark.read \
      .option("compression", "gzip") \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .option("sep", ",") \
      .csv("/FileStore/tables/course_files/csv_gzip/") 

display(df)

# COMMAND ----------

df.printSchema()
