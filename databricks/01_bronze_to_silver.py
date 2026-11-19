# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Parameters and paths

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

dbutils.widgets.text("process_date", "2026-11-01")
process_date = dbutils.widgets.get("process_date")

storage = "storrageproject1"   # storage account name
y, m, d = process_date.split("-")

bronze_path = f"abfss://bronze@{storage}.dfs.core.windows.net/sales/{y}/{m}/{d}/sales_{process_date}.csv"
silver_path = f"abfss://silver@{storage}.dfs.core.windows.net/sales/"

print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema + Raw Dataframe

# COMMAND ----------

schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_email", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("unit_price", StringType()),
    StructField("quantity", StringType()),
    StructField("discount", StringType()),
])

raw = (spark.read
       .option("header", True)
       .option("encoding", "UTF-8")
       .schema(schema)
       .csv(bronze_path))

raw_count = raw.count()
print("Read rows:", raw_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drop duplicates + Clean city column

# COMMAND ----------

df = raw.dropDuplicates()
after_dedup = df.count()

# trim for all text columns
for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c)))

# city: uniform letters, keeping the hyphen (Cluj-Napoca)
df = df.withColumn(
    "city",
    F.regexp_replace(
        F.initcap(F.regexp_replace(F.lower(F.col("city")), "-", " ")),
        " ", "-"
    )
)

print("After dropping duplicates:", after_dedup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Column types 

# COMMAND ----------

df = (df
  .withColumn("order_id", F.col("order_id").cast("int"))
  .withColumn("customer_id", F.col("customer_id").cast("int"))
  .withColumn("product_id", F.col("product_id").cast("int"))
  .withColumn("quantity", F.col("quantity").cast("int"))
  .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
  .withColumn("discount", F.coalesce(F.col("discount").cast("decimal(4,2)"), F.lit(0).cast("decimal(4,2)")))
  .withColumn("customer_email", F.coalesce(F.col("customer_email"), F.lit("unknown")))
  .withColumn("order_date", F.coalesce(
    F.try_to_date("order_date", F.lit("yyyy-MM-dd")),
    F.try_to_date("order_date", F.lit("dd/MM/yyyy"))
))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Filter + Handling NULLS

# COMMAND ----------

valid = df.filter(
    (F.col("quantity") > 0) &
    (F.col("unit_price") > 0) &
    F.col("order_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("order_date").isNotNull()
)

valid_count = valid.count()
print("Final result:", valid_count, "| rejected:", after_dedup - valid_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Writing in Delta format 

# COMMAND ----------

(valid.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .option("replaceWhere", f"order_date = '{process_date}'")
    .save(silver_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final verification

# COMMAND ----------

s = spark.read.format("delta").load(silver_path)
print("Total in silver:", s.count())
print("Key duplicates:", s.count() - s.dropDuplicates(["order_id", "product_id"]).count())
s.groupBy("city").count().show()
s.filter("customer_email = 'unknown'").count()
s.printSchema()

# COMMAND ----------

