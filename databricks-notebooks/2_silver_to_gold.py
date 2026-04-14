# Databricks Notebook: Silver to Gold Transformation
from pyspark.sql.functions import sum, count, avg, desc

storage_account = "retaildatalake2024"
access_key = "YOUR_ACCESS_KEY"

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key)

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/retail/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

# Read Silver tables
customers = spark.read.format("delta").load(silver_path + "customers_delta/")
orders = spark.read.format("delta").load(silver_path + "orders_delta/")
products = spark.read.format("delta").load(silver_path + "products_delta/")
order_items = spark.read.format("delta").load(silver_path + "order_items_delta/")

# Create Daily Sales
daily_sales = orders.groupBy("order_date").agg(
    sum("total_amount").alias("total_sales"),
    count("order_id").alias("total_orders")
)

# Create Customer Spending
customer_spending = customers.join(orders, "customer_id") \
    .groupBy("customer_id", "first_name", "last_name", "customer_segment") \
    .agg(sum("total_amount").alias("total_spent"), count("order_id").alias("total_orders"))

# Save Gold tables
daily_sales.write.format("delta").mode("overwrite").save(gold_path + "daily_sales_delta/")
customer_spending.write.format("delta").mode("overwrite").save(gold_path + "customer_spending_delta/")

print("✅ Silver to Gold transformation complete!")
