# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Parameters and Paths

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("process_date", "2026-11-01")
process_date = dbutils.widgets.get("process_date")
y, m, d = process_date.split("-")

storage = "storrageproject1"   # storage account name
silver_path = f"abfss://silver@{storage}.dfs.core.windows.net/sales/"
gold_path   = f"abfss://gold@{storage}.dfs.core.windows.net/sales/"
export_path = f"abfss://gold@{storage}.dfs.core.windows.net/export/sales/{y}/{m}/{d}/"

print("Processed Day:", process_date)
print("Export:", export_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reading the current day from silver

# COMMAND ----------

day = (spark.read.format("delta").load(silver_path)
       .filter(F.col("order_date") == F.to_date(F.lit(process_date))))

silver_count = day.count()
print("Silver rows for this day:", silver_count)

if silver_count == 0:
    raise Exception(f"Silver does not contain data for {process_date}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. New columns and final DataFrame

# COMMAND ----------

gold = (day
  .withColumn("total_amount",
      F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")), 2).cast("decimal(12,2)"))
  .withColumn("year",  F.year("order_date"))
  .withColumn("month", F.month("order_date"))
  .withColumn("day",   F.dayofmonth("order_date"))
  .select(
      "order_id", "order_date", "year", "month", "day",
      "customer_id", "customer_name", "customer_email", "city", "country",
      "product_id", "product_name", "category",
      "unit_price", "quantity", "discount", "total_amount"
  ))

gold.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verifications before writing

# COMMAND ----------


bad_cust = (gold.groupBy("customer_id")
    .agg(F.countDistinct("customer_name").alias("n"),
         F.countDistinct("city").alias("c"),
         F.countDistinct("country").alias("t"),
         F.countDistinct("customer_email").alias("e"))
    .filter("n > 1 OR c > 1 OR t > 1 OR e > 1").count())

bad_prod = (gold.groupBy("product_id")
    .agg(F.countDistinct("product_name").alias("n"),
         F.countDistinct("category").alias("c"),
         F.countDistinct("unit_price").alias("p"))
    .filter("n > 1 OR c > 1 OR p > 1").count())

print("Inconsistent customers:", bad_cust, "| Inconsistent products:", bad_prod)
if bad_cust > 0 or bad_prod > 0:
    raise Exception("Inconsistent atributes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Writing Delta Table for gold

# COMMAND ----------

(gold.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("order_date")
     .option("replaceWhere", f"order_date = '{process_date}'")
     .save(gold_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Daily export to Parquet

# COMMAND ----------

(gold
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(export_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final verifications

# COMMAND ----------

g = spark.read.format("delta").load(gold_path)
g.groupBy("order_date").count().orderBy("order_date").show()

g.select("order_id", "quantity", "unit_price", "discount", "total_amount").show(5)
print("total_amount <= 0:", g.filter("total_amount <= 0").count())

display(dbutils.fs.ls(export_path))

# COMMAND ----------

