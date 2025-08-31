# ecommerce_glue_clean_csv_final_fixed.py
# Glue 4.0 — newest raw only, 7 clean columns, realistic product names, one CSV (text/csv)

import sys
import uuid
import boto3
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

AWS_REGION       = "us-east-1"
RAW_BUCKET       = "aws-project-4-raw-data"
RAW_PREFIX       = "raw/ecommerce/"                # unified JSONL lives here
OUTPUT_BUCKET    = "aws-project-4-processed-data"  # bucket name only
OUTPUT_BASE_PATH = f"s3://{OUTPUT_BUCKET}/cleaned-csv/ecommerce/"

# ---------- Glue bootstrap ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# ---------- find newest raw JSONL ----------
s3 = boto3.client("s3", region_name=AWS_REGION)
cont = None
latest_key, latest_time = None, datetime(1970, 1, 1, tzinfo=timezone.utc)

while True:
    kw = {"Bucket": RAW_BUCKET, "Prefix": RAW_PREFIX, "MaxKeys": 1000}
    if cont:
        kw["ContinuationToken"] = cont
    resp = s3.list_objects_v2(**kw)
    for o in resp.get("Contents", []):
        if o["Key"].endswith(".jsonl") and o["LastModified"] > latest_time:
            latest_key, latest_time = o["Key"], o["LastModified"]
    if not resp.get("IsTruncated"):
        break
    cont = resp.get("NextContinuationToken")

if not latest_key:
    print("No JSONL files found; exiting.")
    job.commit()
    sys.exit(0)

raw_path = f"s3://{RAW_BUCKET}/{latest_key}"
print("Reading newest raw:", raw_path)

# ---------- read unified JSONL ----------
df = spark.read.json(raw_path)

# ---------- project orders / payments ----------
orders = (
    df.filter(F.col("record_type") == "order")
      .select(
          F.col("customer_id"),
          F.trim("order_id").alias("order_id"),
          "order_date","product","quantity","price","discount_pct","order_amount"
      )
)

payments = (
    df.filter(F.col("record_type") == "payment")
      .select(
          F.col("order_id").alias("p_order_id"),
          F.col("status").alias("p_payment_status")
      )
)

# ---------- shorten customer_id (stable short ID) ----------
short_id = F.concat(
    F.lit("CUST-"),
    F.lpad((F.abs(F.xxhash64(F.col("customer_id"))) % F.lit(1000000)).cast("string"), 6, "0")
)
orders = orders.withColumn("customer_id_short", short_id)

# ---------- core transforms ----------
orders = (
    orders
    .withColumn("order_ts", F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_date_std", F.date_format("order_ts", "yyyy-MM-dd"))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("discount_pct", F.coalesce(F.col("discount_pct").cast(DoubleType()), F.lit(0.0)))
    .withColumn("order_amount", F.col("order_amount").cast(DoubleType()))
    .withColumn(
        "order_amount_filled",
        F.when(
            (F.col("order_amount").isNull()) | (F.col("order_amount") <= 0),
            F.round(F.col("price") * F.col("quantity") * (1 - F.col("discount_pct")/100.0), 2)
        ).otherwise(F.round(F.col("order_amount"), 2))
    )
    .filter(
        (F.col("order_id").isNotNull()) &
        (F.col("quantity") >= 1) &
        (F.col("order_amount_filled") >= 0)
    )
)

# ---------- join payments ----------
orders_enriched = (
    orders.join(payments, orders.order_id == payments.p_order_id, "left")
          .drop("p_order_id")
          .withColumn("payment_status", F.lower(F.coalesce(F.col("p_payment_status"), F.lit("unknown"))))
          .drop("p_payment_status")
)

# ---------- FORCE realistic product names (deterministic by order_id; index cast to INT) ----------
orders_enriched = orders_enriched.withColumn(
    "catalog",
    F.array(
        F.lit("iPhone 14"), F.lit("Galaxy S23"), F.lit("Pixel 7 Pro"),
        F.lit("MacBook Pro"), F.lit("ThinkPad X1 Carbon"), F.lit("AirPods Pro"),
        F.lit("Sony WH-1000XM5"), F.lit("Canon EOS R5"), F.lit("Nikon Z6 II"),
        F.lit("Kindle Paperwhite"), F.lit("Echo Dot"), F.lit("Apple Watch Series 8")
    )
)

# element_at(array, index:int) is 1-based → cast the index to INT explicitly
orders_enriched = orders_enriched.withColumn(
    "product_forced",
    F.expr("element_at(catalog, CAST((abs(xxhash64(order_id)) % size(catalog)) AS INT) + 1)")
).drop("catalog")

# ---------- final 7 columns ----------
clean7 = orders_enriched.select(
    "order_id",
    F.col("customer_id_short").alias("customer_id"),
    F.col("order_date_std").alias("order_date"),
    F.col("product_forced").alias("product"),
    "quantity",
    F.round(F.col("order_amount_filled"), 2).alias("order_amount"),
    "payment_status"
)

# ---------- write ONE CSV (and only CSV) ----------
batch_id = uuid.uuid4().hex
ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
out_prefix = f"{OUTPUT_BASE_PATH}final_batch_{batch_id}/"

(clean7.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", True)
       .option("quoteAll", True)
       .option("escape", "\"")
       .option("nullValue", "")
       .option("emptyValue", "")
       .csv(out_prefix))

print("CSV written under:", out_prefix)

# ---------- rename part file -> ecommerce_clean_<ts>.csv and set Content-Type ----------
prefix = f"cleaned-csv/ecommerce/final_batch_{batch_id}/"
try:
    resp = s3.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=prefix)
    part_key = None
    for o in resp.get("Contents", []):
        k = o["Key"]
        if k.endswith(".csv") and "/_SUCCESS" not in k:
            part_key = k
            break

    if part_key:
        final_key = f"{prefix}ecommerce_clean_{ts}.csv"
        s3.copy_object(
            Bucket=OUTPUT_BUCKET,
            CopySource={"Bucket": OUTPUT_BUCKET, "Key": part_key},
            Key=final_key,
            ContentType="text/csv",
            MetadataDirective="REPLACE"
        )
        try:
            s3.delete_object(Bucket=OUTPUT_BUCKET, Key=part_key)
        except Exception as e:
            print("Delete original part file not permitted:", e)
        print("Renamed to:", f"s3://{OUTPUT_BUCKET}/{final_key}")
    else:
        print("No part-*.csv found to rename.")
except Exception as e:
    print("Rename step non-fatal error:", e)

job.commit()
print("Done.")
