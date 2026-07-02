# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a03cfff1-048d-457c-8848-da958470832d",
# META       "default_lakehouse_name": "lh_silver_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "a03cfff1-048d-457c-8848-da958470832d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# 1. Load Bronze
df = spark.table("bronze_account_product_enrollments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardise column names

df = df.select([F.col(c).alias(c.lower().strip()) for c in df.columns])


# Type casting 

df = df.withColumn("account_id", F.col("account_id").cast("string")) \
       .withColumn("product_code", F.col("product_code").cast("string")) \
       .withColumn("enrollment_date", F.to_date("enrollment_date")) \
       .withColumn("ingested_at", F.to_timestamp("ingested_at"))


# Deduplication 

window_spec = Window.partitionBy("account_id", "product_code") \
                    .orderBy(F.col("ingested_at").desc())

df = df.withColumn("rn", F.row_number().over(window_spec)) \
       .filter(F.col("rn") == 1) \
       .drop("rn")

# -----------------------------
# 5. Standardise status values
# -----------------------------
df = df.withColumn(
    "enrollment_status",
    F.lower(F.trim(F.col("enrollment_status")))
)



# -----------------------------
# 8. Data quality flags
# -----------------------------
df = df.withColumn(
    "dq_has_account",
    F.col("account_id").isNotNull()
).withColumn(
    "dq_has_product",
    F.col("product_code").isNotNull()
).withColumn(
    "dq_has_date",
    F.col("enrollment_date").isNotNull()
)

# -----------------------------
# 9. Audit columns (standard silver pattern)
# -----------------------------
df = df.withColumn("silver_load_ts", F.current_timestamp()) \
       .withColumn("source_table", F.lit("bronze_account_product_enrollments"))

# -----------------------------
# 10. Optional: enforce business key integrity
# -----------------------------
df = df.filter(
    F.col("account_id").isNotNull() &
    F.col("product_code").isNotNull()
)

# -----------------------------
# 11. Write to Silver
# -----------------------------
df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("silver_account_product_enrollments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Derived business flags

df = df.withColumn(
    "is_active",
    F.when(F.col("enrollment_status") == "active", 1).otherwise(0)
)

df = df.withColumn(
    "is_closed",
    F.when(F.col("enrollment_status").isin("cancelled", "suspended"), 1).otherwise(0)
)

df = df.withColumn(
    "is_suspended",
    F.when(F.col("enrollment_status") == "suspended", 1).otherwise(0)
)


# Product grouping 
df = df.withColumn(
    "product_category",
    F.when(F.col("product_code").isin("online_banking", "debit_card"), "core_banking")
     .when(F.col("product_code").isin("credit_card", "overdraft_facility"), "credit_products")
     .when(F.col("product_code").isin("investment_account", "wealth_management"), "investment")
     .otherwise("other")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
