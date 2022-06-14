from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#   Initialize the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()


#   Read the source tables in Parquet format
products_table = spark.read.parquet("./products_parquet")
sales_table = spark.read.parquet("./sales_parquet")
sellers_table = spark.read.parquet("./sellers_parquet")


#   Output how many distinct products have been sold in each date
print("how many distinct products have been sold in each date")
sales_table.groupby(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(
    col("distinct_products_sold").desc()).show()

spark.stop()
#
