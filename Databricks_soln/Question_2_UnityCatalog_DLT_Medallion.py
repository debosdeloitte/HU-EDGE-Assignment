# Databricks notebook source
spark.sql("""CREATE CATALOG IF NOT EXISTS ecommerce""")
spark.sql("""CREATE SCHEMA  IF NOT EXISTS ecommerce.raw""")

#CREATE TABLE IF NOT EXISTS ecommerce.raw.users
#USING CSV OPTIONS (path '/Volumes/main/ecommerce/raw/users.csv', header 'true');

# automated code   
opts = dict(header=True, multiLine=True, escape='"', inferSchema=True)
BASE = "/Volumes/ecommerce/raw/databricks_dataset"   
for t in ["users","sales","products","events"]:
    (spark.read.csv(f"{BASE}/{t}.csv", **opts)
        .write.mode("overwrite").saveAsTable(f"ecommerce.raw.{t}_part_2"))
    
display(spark.sql(""" show tables in ecommerce.raw """))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data scientists: read-only on sales & events
# MAGIC GRANT SELECT ON TABLE ecommerce.raw.sales_part_2  TO `debosghosh@deloitte.com`;
# MAGIC GRANT SELECT ON TABLE ecommerce.raw.events_part_2 TO `debosghosh@deloitte.com`;
# MAGIC
# MAGIC -- Data engineers: full access to every table in the schema
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ecommerce.raw TO `debosghosh@deloitte.com`;
# MAGIC
# MAGIC -- Interns: products only
# MAGIC GRANT SELECT ON TABLE ecommerce.raw.products_part_2 TO `debosghosh@deloitte.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_USER()

# COMMAND ----------

# MAGIC %md
# MAGIC # Audit & lineage (system tables)

# COMMAND ----------

for t in ["users","sales","products","events"]:
    print (f"----- Table access and modification details : {t} -------")
    df =spark.sql(f"DESCRIBE HISTORY ecommerce.raw.{t}_part_2")
    display(df)