# Databricks Notebook: Bronze to Silver Transformation (Complete)
# ================================================================

from pyspark.sql.functions import col, concat, lit, year, month, dayofmonth, when, current_date
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Configuration
storage_account = "retaildatalake2024"
access_key = "YOUR_ACCESS_KEY"  # Replace with your actual key

# Configure Spark
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key)

# Paths
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/onprem_retail_db/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/retail/"

print("="*60)
print("BRONZE TO SILVER TRANSFORMATION")
print("="*60)

# Step 1: Read Bronze Layer
print("\n📖 Reading Bronze tables...")

customers = spark.read.parquet(bronze_path + "customers/")
orders = spark.read.parquet(bronze_path + "orders/")
products = spark.read.parquet(bronze_path + "products/")
order_items = spark.read.parquet(bronze_path + "order_items/")

print(f"✅ Customers: {customers.count()} rows")
print(f"✅ Orders: {orders.count()} rows")
print(f"✅ Products: {products.count()} rows")
print(f"✅ Order Items: {order_items.count()} rows")

# Step 2: Transform Customers
print("\n🔄 Transforming Customers...")

customers_silver = customers \
    .dropDuplicates(["customer_id"]) \
    .fillna({
        "phone": "Unknown",
        "address_line1": "N/A",
        "address_line2": "N/A",
        "city": "Unknown",
        "state": "Unknown",
        "postal_code": "00000"
    }) \
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
    .withColumn("is_active_str", when(col("is_active") == True, "Active").otherwise("Inactive")) \
    .withColumn("customer_tenure_days", datediff(current_date(), col("registration_date")))

print("✅ Customers transformed")

# Step 3: Transform Orders
print("\n🔄 Transforming Orders...")

orders_silver = orders \
    .dropDuplicates(["order_id"]) \
    .filter(col("status") != "Cancelled") \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_day", dayofmonth(col("order_date")))

print("✅ Orders transformed")

# Step 4: Transform Products
print("\n🔄 Transforming Products...")

products_silver = products \
    .dropDuplicates(["product_id"]) \
    .fillna({"brand": "Unknown", "description": "No description"}) \
    .withColumn("profit_margin", 
                (col("unit_price") - col("cost")) / col("unit_price") * 100) \
    .withColumn("profit_margin_rounded", round(col("profit_margin"), 2))

print("✅ Products transformed")

# Step 5: Transform Order Items
print("\n🔄 Transforming Order Items...")

order_items_silver = order_items \
    .dropDuplicates(["order_item_id"]) \
    .withColumn("line_total", 
                col("quantity") * col("unit_price") - col("discount")) \
    .withColumn("discount_percentage", 
                col("discount") / (col("quantity") * col("unit_price")) * 100)

print("✅ Order Items transformed")

# Step 6: Create Joined Order Details
print("\n🔗 Creating Order Details (Joined View)...")

order_details_silver = orders_silver.join(
    customers_silver.select("customer_id", "first_name", "last_name", "full_name", "email", "customer_segment"),
    "customer_id",
    "inner"
).join(
    order_items_silver.select("order_id", "product_id", "quantity", "line_total"),
    "order_id",
    "inner"
).join(
    products_silver.select("product_id", "product_name", "category", "brand", "unit_price", "profit_margin"),
    "product_id",
    "inner"
)

print(f"✅ Order Details: {order_details_silver.count()} rows")

# Step 7: Write to Silver Layer (Delta Format)
print("\n💾 Writing to Silver Layer (Delta)...")

customers_silver.write.format("delta").mode("overwrite").save(silver_path + "customers_delta/")
orders_silver.write.format("delta").mode("overwrite").save(silver_path + "orders_delta/")
products_silver.write.format("delta").mode("overwrite").save(silver_path + "products_delta/")
order_items_silver.write.format("delta").mode("overwrite").save(silver_path + "order_items_delta/")
order_details_silver.write.format("delta").mode("overwrite").save(silver_path + "order_details_delta/")

print("✅ All Silver tables saved as Delta")

# Step 8: Verification
print("\n📊 Verification:")
print(f"   Customers: {customers_silver.count()} rows")
print(f"   Orders: {orders_silver.count()} rows")
print(f"   Products: {products_silver.count()} rows")
print(f"   Order Items: {order_items_silver.count()} rows")
print(f"   Order Details: {order_details_silver.count()} rows")

print("\n✅ BRONZE TO SILVER TRANSFORMATION COMPLETE!")
