from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from pyspark import SparkFiles
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("Spark UI name")
    .config("hive.merge.sparkfiles", "true")
    .config("parquet.compression", "snappy")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.broadcastTimeout", 300)
    .enableHiveSupport()
    .getOrCreate())

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark.sparkContext.addPyFile(custom_lib_path)
spark.sparkContext.addFile(file_path)
with open(SparkFiles.get("file")) as f:
    pass
spark.catalog.listDatabases()
spark.catalog.listTables("database_name")
spark.catalog.listColumns("table_name", dbName="database_name")
spark.sql("SHOW PARTITIONS database_name.table_name").collect()

schema = T.StructType([
    T.StructField("column_name_string", T.StringType(), True),
    T.StructField("column_name_bigint", T.IntegerType(), True),
    T.StructField("column_name_decimal", T.DecimalType(), True)])

def example_function(x):
    return x
example_function_udf = F.udf(example_function, T.IntegerType())

df = spark.sql("SELECT * FROM database_name.table_name")
df = spark.table("database_name.table_name")

df.select("column_name")
df.first()
df.take(5)
df.show()
df.count()
df.cache()
df.createOrReplaceTempView("view_name")

df_show_as_string = df._jdf.showstring(2, 20)

(df.groupBy("column_name_group")
    .agg(
        F.countDistinct("column_name").alias("column_name")
        ))

df.join(df2, ["id_col_name"], "left")

(df.filter(F.col("dt").between(dt_from, dt_to))
    .withColumn("rn",
        F.row_number().over(
            F.Window.partitionBy("col1")
            .orderBy("col2"))))

df.toPandas()

spark.catalog.refreshTable("database_name.table_name")

df.write.saveAsTable("database_name.table_name", mode="overwrite")

(df.coalesce(1)
    .insertInto("database_name.table_name", overwrite=True))

(df.coalesce(1)
    .write.option("compression", "snappy")
    .format("parquet")
    .insertInto("database_name.table_name", overwrite=True))
