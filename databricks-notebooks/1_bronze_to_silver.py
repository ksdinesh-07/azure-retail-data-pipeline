# Databricks Notebook: Bronze to Silver Transformation
from pyspark.sql.functions import col, concat, lit, year, month, dayofmonth, when

# Configure storage access
storage_account = "retaildatalake2024"
access_key = "YOUR_ACCESS_KEY"

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key)

# Paths
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/onprem_retail_db/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/retail/"

# Read from Bronze
customers = spark.read.parquet(bronze_path + "customers/")
orders = spark.read.parquet(bronze_path + "orders/")
products = spark.read.parquet(bronze_path + "products/")
order_items = spark.read.parquet(bronze_path + "order_items/")

# Transform Customers
customers_clean = customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
    .withColumn("is_active_str", when(col("is_active") == True, "Active").otherwise("Inactive"))

# Transform Orders
orders_clean = orders \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_day", dayofmonth(col("order_date")))

# Write to Silver (Delta format)
customers_clean.write.format("delta").mode("overwrite").save(silver_path + "customers_delta/")
orders_clean.write.format("delta").mode("overwrite").save(silver_path + "orders_delta/")

print("✅ Bronze to Silver transformation complete!")
