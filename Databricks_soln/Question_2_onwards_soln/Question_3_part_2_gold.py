# GOLD 1: Monthly revenue by product and city.
# Revenue + product + city co-exist only on purchase (finalize) events, so we
# attribute completed-purchase revenue by month/product/city from those.
# PERF: partition by `month` (query filter), Z-ORDER by item_id & city (the join/
# filter keys) via the pipelines.autoOptimize.zOrderCols table property.
@dlt.table(
    comment="Monthly purchase revenue by product and city",
    partition_cols=["month"],
    table_properties={**EVOLVE, "pipelines.autoOptimize.zOrderCols": "item_id,city"})
def gold_monthly_revenue_by_product_city():
    return (dlt.read("silver_event_items")
        .filter(F.col("event_name") == "finalize")
        .withColumn("month", F.date_format("event_timestamp", "yyyy-MM"))
        .groupBy("month", "item_id", "city")
        .agg(F.round(F.sum("item_revenue_in_usd"), 2).alias("revenue")))

# GOLD 2: Top 10 customers by lifetime value.
@dlt.table(comment="Top 10 customers by lifetime value")
def gold_top10_customers_by_ltv():
    return (dlt.read("bronze_sales")
        .groupBy("email")
        .agg(F.round(F.sum("purchase_revenue_in_usd"), 2).alias("clv"),
             F.countDistinct("order_id").alias("orders"))
        .orderBy(F.desc("clv"))
        .limit(10))

# GOLD 3: Conversion rate by traffic source.
# conversion = distinct users who reached a `finalize` (purchase) / distinct visitors.
@dlt.table(comment="Conversion rate by traffic source")
def gold_conversion_by_traffic_source():
    ev = dlt.read("bronze_events")
    visitors = ev.groupBy("traffic_source").agg(F.countDistinct("user_id").alias("visitors"))
    buyers   = (ev.filter(F.col("event_name") == "finalize")
                  .groupBy("traffic_source").agg(F.countDistinct("user_id").alias("buyers")))
    return (visitors.join(buyers, "traffic_source", "left")
            .withColumn("conversion_rate",
                        F.round(F.coalesce("buyers", F.lit(0)) / F.col("visitors"), 4))
            .orderBy(F.desc("conversion_rate")))

# Optional: keep your customer-lifetime-value detail table (feeds Gold 2 conceptually)
@dlt.table(comment="Customer lifetime value (all customers)")
@dlt.expect_or_fail("positive_clv", "clv >= 0")
def gold_customer_ltv():
    s = dlt.read("bronze_sales")
    u = dlt.read("bronze_users")
    return (s.join(u, "email", "left")
             .groupBy("email")
             .agg(F.sum("purchase_revenue_in_usd").alias("clv"),
                  F.countDistinct("order_id").alias("orders")))