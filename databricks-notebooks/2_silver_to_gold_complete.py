# Databricks Notebook: Silver to Gold Transformation (Complete)
# ============================================================

from pyspark.sql.functions import sum, count, avg, countDistinct, desc, col, round as spark_round

# Configuration
storage_account = "retaildatalake2024"
access_key = "YOUR_ACCESS_KEY"  # Replace with your actual key

# Configure Spark
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key)

# Paths
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/retail/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print("="*60)
print("SILVER TO GOLD TRANSFORMATION")
print("="*60)

# Step 1: Read Silver Layer
print("\n📖 Reading Silver tables...")

customers = spark.read.format("delta").load(silver_path + "customers_delta/")
orders = spark.read.format("delta").load(silver_path + "orders_delta/")
products = spark.read.format("delta").load(silver_path + "products_delta/")
order_items = spark.read.format("delta").load(silver_path + "order_items_delta/")
order_details = spark.read.format("delta").load(silver_path + "order_details_delta/")

print("✅ Silver tables loaded")

# Step 2: Gold Table 1 - Daily Sales Summary
print("\n📊 Creating Daily Sales Summary...")

daily_sales = order_details.groupBy("order_date") \
    .agg(
        sum("line_total").alias("total_sales"),
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_items_sold"),
        avg("line_total").alias("avg_order_value")
    ) \
    .fillna({"total_sales": 0, "total_orders": 0, "total_items_sold": 0}) \
    .orderBy("order_date")

daily_sales.write.format("delta").mode("overwrite").save(gold_path + "daily_sales_delta/")
print(f"✅ Daily Sales: {daily_sales.count()} rows")

# Step 3: Gold Table 2 - Customer Spending Analysis
print("\n📊 Creating Customer Spending Analysis...")

customer_spending = order_details.groupBy("customer_id", "first_name", "last_name", "email", "customer_segment") \
    .agg(
        sum("line_total").alias("total_spent"),
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_items"),
        avg("line_total").alias("avg_order_value")
    ) \
    .fillna({"total_spent": 0, "total_orders": 0, "total_items": 0}) \
    .orderBy(desc("total_spent"))

customer_spending.write.format("delta").mode("overwrite").save(gold_path + "customer_spending_delta/")
print(f"✅ Customer Spending: {customer_spending.count()} rows")

# Step 4: Gold Table 3 - Product Performance
print("\n📊 Creating Product Performance...")

product_performance = order_details.groupBy("product_id", "product_name", "category", "brand") \
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("times_ordered"),
        sum("line_total").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .fillna({"total_quantity_sold": 0, "times_ordered": 0, "total_revenue": 0}) \
    .withColumn("revenue_per_unit", 
                col("total_revenue") / col("total_quantity_sold")) \
    .orderBy(desc("total_revenue"))

product_performance.write.format("delta").mode("overwrite").save(gold_path + "product_performance_delta/")
print(f"✅ Product Performance: {product_performance.count()} rows")

# Step 5: Gold Table 4 - Category Summary
print("\n📊 Creating Category Summary...")

category_summary = order_details.groupBy("category") \
    .agg(
        sum("line_total").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("order_id").alias("total_orders")
    ) \
    .fillna({"total_revenue": 0, "total_quantity": 0}) \
    .orderBy(desc("total_revenue"))

category_summary.write.format("delta").mode("overwrite").save(gold_path + "category_summary_delta/")
print(f"✅ Category Summary: {category_summary.count()} rows")

# Step 6: Gold Table 5 - Status Summary
print("\n📊 Creating Status Summary...")

status_summary = order_details.groupBy("status") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("line_total").alias("total_revenue"),
        avg("line_total").alias("avg_order_value")
    ) \
    .fillna({"total_orders": 0, "total_revenue": 0}) \
    .orderBy(desc("total_orders"))

status_summary.write.format("delta").mode("overwrite").save(gold_path + "status_summary_delta/")
print(f"✅ Status Summary: {status_summary.count()} rows")

# Step 7: Verification
print("\n" + "="*60)
print("📊 GOLD LAYER VERIFICATION")
print("="*60)

print(f"   Daily Sales: {daily_sales.count()} rows")
print(f"   Customer Spending: {customer_spending.count()} rows")
print(f"   Product Performance: {product_performance.count()} rows")
print(f"   Category Summary: {category_summary.count()} rows")
print(f"   Status Summary: {status_summary.count()} rows")

print("\n✅ SILVER TO GOLD TRANSFORMATION COMPLETE!")
