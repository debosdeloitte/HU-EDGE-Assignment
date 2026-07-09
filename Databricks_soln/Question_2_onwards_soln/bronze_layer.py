# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

BASE = "/Volumes/ecommerce/raw/databricks_dataset"

spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.bronze")

items_schema = ArrayType(StructType([
    StructField("coupon", StringType()), StructField("item_id", StringType()),
    StructField("item_name", StringType()), StructField("item_revenue_in_usd", DoubleType()),
    StructField("price_in_usd", DoubleType()), StructField("quantity", LongType())]))
geo_schema  = StructType([StructField("city", StringType()), StructField("state", StringType())])
ecom_schema = StructType([StructField("purchase_revenue_in_usd", DoubleType()),
    StructField("total_item_quantity", LongType()), StructField("unique_items", LongType())])

# events
(spark.read.csv(f"{BASE}/events.csv", header=True, multiLine=True, nullValue="null", escape='"')
    .withColumn("items",     F.from_json("items",     items_schema))
    .withColumn("geo",       F.from_json("geo",       geo_schema))
    .withColumn("ecommerce", F.from_json("ecommerce", ecom_schema))
    .withColumn("event_timestamp",            (F.col("event_timestamp")/1e6).cast("timestamp"))
    .withColumn("event_previous_timestamp",   (F.col("event_previous_timestamp")/1e6).cast("timestamp"))
    .withColumn("user_first_touch_timestamp", (F.col("user_first_touch_timestamp")/1e6).cast("timestamp"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce.bronze.events_question3"))

# sales
(spark.read.csv(f"{BASE}/sales.csv", header=True, multiLine=True, escape='"', nullValue="null", inferSchema=True)
    .withColumn("items", F.from_json("items", items_schema))
    .withColumn("transaction_timestamp", (F.col("transaction_timestamp")/1e6).cast("timestamp"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce.bronze.sales_question3"))

# users + products
(spark.read.csv(f"{BASE}/users.csv", header=True, multiLine=True, nullValue="null", inferSchema=True)
    .withColumn("user_first_touch_timestamp", (F.col("user_first_touch_timestamp")/1e6).cast("timestamp"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce.bronze.users_question3"))

(spark.read.csv(f"{BASE}/products.csv", header=True, multiLine=True, nullValue="null", inferSchema=True)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce.bronze.products_question3"))