#   What is the average revenue of the orders?

from pyspark.sql import SparkSession

# Create the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .config("spark.ui.port","4040")\
    .appName("Exercise1") \
    .getOrCreate()

# Read the source tables
products_table = spark.read.parquet("./products_parquet")
sales_table = spark.read.parquet("./sales_parquet")
sellers_table = spark.read.parquet("./sellers_parquet")

# Do the join and print the results
print(sales_table.join(products_table, sales_table["product_id"] == products_table["product_id"], "inner").
      agg(avg(products_table["price"] * sales_table["num_pieces_sold"])).show())

spark.stop()
