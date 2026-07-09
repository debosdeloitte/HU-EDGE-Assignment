# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.silver")

# sales: dedup + flatten items + enrich with products  (items is already array<struct> from bronze)
sales    = spark.table("ecommerce.bronze.sales_question3").dropDuplicates(["order_id"])
products = spark.table("ecommerce.bronze.products_question3").select(
    "item_id", F.col("name").alias("product_name"), F.col("price").alias("product_price"))

(sales
    .select("order_id", "email", "transaction_timestamp", F.explode("items").alias("i"))
    .select("order_id", "email", "transaction_timestamp", "i.*")
    .join(products, "item_id", "left")
    .write.mode("overwrite").saveAsTable("ecommerce.silver.sales_enriched_question3"))

# cleansed events (quality filter) + flattened event items with city
events = spark.table("ecommerce.bronze.events").dropDuplicates().filter(F.col("user_id").isNotNull())
events.write.mode("overwrite").saveAsTable("ecommerce.silver.events_question3")

(events
    .select("user_id", "event_name", "traffic_source", "event_timestamp",
            F.col("geo.city").alias("city"), F.explode("items").alias("i"))
    .select("user_id", "event_name", "traffic_source", "event_timestamp", "city", "i.*")
    .write.mode("overwrite").saveAsTable("ecommerce.silver.event_items_question3"))