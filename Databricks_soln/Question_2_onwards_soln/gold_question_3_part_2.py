from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.gold")

# 1. Monthly revenue by product and city (from finalize purchase events)
(spark.table("ecommerce.raw.silver_event_items_dlt_q3_part2")
    .filter(F.col("event_name") == "finalize")
    .withColumn("month", F.date_format("event_timestamp", "yyyy-MM"))
    .groupBy("month", "item_id", "city")
    .agg(F.round(F.sum("item_revenue_in_usd"), 2).alias("revenue"))
    .write.mode("overwrite").partitionBy("month")
    .saveAsTable("ecommerce.gold.monthly_revenue_by_product_city_question_3_part_2"))

# 2. Top 10 customers by lifetime value
(spark.table("ecommerce.raw.bronze_sales_dlt_q3_part2")
    .groupBy("email")
    .agg(F.round(F.sum("purchase_revenue_in_usd"), 2).alias("clv"),
         F.countDistinct("order_id").alias("orders"))
    .orderBy(F.desc("clv")).limit(10)
    .write.mode("overwrite").saveAsTable("ecommerce.gold.top10_customers_by_ltv_question_3_part_2"))

# 3. Conversion rate by traffic source
ev       = spark.table("ecommerce.raw.silver_events_dlt_q3_part2")
visitors = ev.groupBy("traffic_source").agg(F.countDistinct("user_id").alias("visitors"))
buyers   = (ev.filter(F.col("event_name") == "finalize")
              .groupBy("traffic_source").agg(F.countDistinct("user_id").alias("buyers")))
(visitors.join(buyers, "traffic_source", "left")
    .withColumn("conversion_rate", F.round(F.coalesce("buyers", F.lit(0)) / F.col("visitors"), 4))
    .orderBy(F.desc("conversion_rate"))
    .write.mode("overwrite").saveAsTable("ecommerce.gold.conversion_by_traffic_source_question_3_part_2"))