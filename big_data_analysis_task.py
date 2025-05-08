from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
import pandas as pd

# Start Spark session
spark = SparkSession.builder.appName("Big Data Analysis Task").getOrCreate()

# Sample data
sample_data = [
    ("ORD1", "Laptop", "Electronics", 2, 1500, "2024-01-05", "CUST101"),
    ("ORD2", "Phone", "Electronics", 1, 1000, "2024-01-06", "CUST102"),
    ("ORD3", "Tablet", "Electronics", 3, 1200, "2024-01-07", "CUST103"),
    ("ORD4", "Headphones", "Accessories", 5, 200, "2024-01-08", "CUST104"),
    ("ORD5", "Charger", "Accessories", 4, 50, "2024-01-09", "CUST105"),
]
columns = ["OrderID", "Product", "Category", "Quantity", "Price", "OrderDate", "CustomerID"]
df = spark.createDataFrame(sample_data, columns)

# Add a column for Total Price
df = df.withColumn("TotalPrice", col("Quantity") * col("Price"))

# Total Sales Revenue
total_sales_df = df.groupBy().agg(spark_sum("TotalPrice").alias("TotalSalesRevenue"))

# Top Selling Products by Quantity Sold
top_products_df = df.groupBy("Product").agg(
    spark_sum("Quantity").alias("TotalQuantitySold")
).orderBy(col("TotalQuantitySold").desc())

# Revenue by Category
revenue_by_category_df = df.groupBy("Category").agg(
    spark_sum("TotalPrice").alias("CategoryRevenue")
).orderBy(col("CategoryRevenue").desc())

# Orders Count by Customer
orders_by_customer_df = df.groupBy("CustomerID").agg(
    count("OrderID").alias("TotalOrders")
).orderBy(col("TotalOrders").desc())

# Convert results to Pandas for saving
total_sales_pd = total_sales_df.toPandas()
top_products_pd = top_products_df.toPandas()
revenue_by_category_pd = revenue_by_category_df.toPandas()
orders_by_customer_pd = orders_by_customer_df.toPandas()

# Save each output to separate sheets in one Excel file
output_path = "/mnt/data/big_data_analysis_output.xlsx"
with pd.ExcelWriter(output_path) as writer:
    total_sales_pd.to_excel(writer, sheet_name="Total_Sales", index=False)
    top_products_pd.to_excel(writer, sheet_name="Top_Products", index=False)
    revenue_by_category_pd.to_excel(writer, sheet_name="Revenue_by_Category", index=False)
    orders_by_customer_pd.to_excel(writer, sheet_name="Orders_by_Customer", index=False)

# Stop the Spark session
spark.stop()

output_path
