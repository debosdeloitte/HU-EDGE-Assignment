# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *



BASE = "/Volumes/ecommerce/raw/databricks_dataset"

# Schemas for JSON string columns
items_schema = ArrayType(StructType([
    StructField("coupon", StringType()),
    StructField("item_id", StringType()),
    StructField("item_name", StringType()),
    StructField("item_revenue_in_usd", DoubleType()),
    StructField("price_in_usd", DoubleType()),
    StructField("quantity", LongType()),
]))
geo_schema  = StructType([StructField("city", StringType()), StructField("state", StringType())])
ecom_schema = StructType([
    StructField("purchase_revenue_in_usd", DoubleType()),
    StructField("total_item_quantity", LongType()),
    StructField("unique_items", LongType()),
])

# common table property: let Delta merge new columns as the source schema evolves
EVOLVE = {"delta.autoOptimize.optimizeWrite": "true",
          "delta.autoOptimize.autoCompact": "true"}


# BRONZE LAYER  — raw ingest into Delta


@dlt.table(comment="Raw events from CSV", table_properties=EVOLVE)
def bronze_events_question_3_part_2():
    return (spark.read.csv(f"{BASE}/events.csv", header=True, multiLine=True, nullValue="null", escape='"')
            .withColumn("items",     F.from_json("items",     items_schema))
            .withColumn("geo",       F.from_json("geo",       geo_schema))
            .withColumn("ecommerce", F.from_json("ecommerce", ecom_schema))
            .withColumn("event_timestamp",            (F.col("event_timestamp")/1e6).cast("timestamp"))
            .withColumn("event_previous_timestamp",   (F.col("event_previous_timestamp")/1e6).cast("timestamp"))
            .withColumn("user_first_touch_timestamp", (F.col("user_first_touch_timestamp")/1e6).cast("timestamp")))

@dlt.table(comment="Raw sales from CSV", table_properties=EVOLVE)
def bronze_sales_question_3_part_2():
    return (spark.read.csv(f"{BASE}/sales.csv", header=True, multiLine=True, escape='"', nullValue="null", inferSchema=True)
            .withColumn("items", F.from_json("items", items_schema))
            .withColumn("transaction_timestamp", (F.col("transaction_timestamp")/1e6).cast("timestamp")))

@dlt.table(comment="Raw users from CSV", table_properties=EVOLVE)
def bronze_users_question_3_part_2():
    return (spark.read.csv(f"{BASE}/users.csv", header=True, multiLine=True, nullValue="null", inferSchema=True)
            .withColumn("user_first_touch_timestamp", (F.col("user_first_touch_timestamp")/1e6).cast("timestamp")))


@dlt.table(comment="Raw products (price catalog) from CSV", table_properties=EVOLVE)
def bronze_products_question_3_part_2():
    return spark.read.csv(f"{BASE}/products.csv", header=True, multiLine=True, nullValue="null", inferSchema=True)


# SILVER LAYER  — cleanse + enrich



# Cleansed events with data-quality expectations
@dlt.table(comment="Cleansed events")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect("non_null_ts", "event_timestamp IS NOT NULL")
def silver_events_question_3_part_2():
    return dlt.read("bronze_events").dropDuplicates()


@dlt.table(comment="Sales cleansed (dedup on order_id), flattened to item grain, enriched with product catalog")
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
def silver_sales_enriched_question_3_part_2():
    sales = dlt.read("bronze_sales").dropDuplicates(["order_id"])           # remove duplicates
    products = dlt.read("bronze_products").select(
        "item_id",
        F.col("name").alias("product_name"),
        F.col("price").alias("product_price"))
    return (sales
        .select("order_id", "email", "transaction_timestamp", F.explode("items").alias("i"))  # flatten items
        .select("order_id", "email", "transaction_timestamp", "i.*")
        .join(products, "item_id", "left"))                                 # enrich with products

# Flattened event items (kept from your Part-1 pattern; used by Gold conversion + revenue)
@dlt.table(comment="Flattened event items")
def silver_event_items_question_3_part_2():
    return (dlt.read("bronze_events")
            .select("user_id", "event_name", "traffic_source", "event_timestamp",
                    F.col("geo.city").alias("city"), F.explode("items").alias("i"))
            .select("user_id", "event_name", "traffic_source", "event_timestamp", "city", "i.*"))


