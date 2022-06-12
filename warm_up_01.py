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

#   Print the number of orders
print("Number of Orders: {}".format(sales_table.count()))

#   Print the number of sellers
print("Number of sellers: {}".format(sellers_table.count()))

#   Print the number of products
print("Number of products: {}".format(products_table.count()))
