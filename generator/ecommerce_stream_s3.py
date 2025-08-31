import boto3
import json
import random
import uuid
import string
import time
from datetime import datetime, timedelta
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError

# =======================
# CONFIG
# =======================
AWS_REGION  = "us-east-1"
BUCKET_NAME = "aws-project-4-raw-data"
BASE_PREFIX = "raw/ecommerce"

CUSTOMER_POOL_SIZE = 200
ORDERS_COUNT       = 1000
DAYS_HISTORY       = 30

# Retry
MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0

# =======================
# Retry Helper
# =======================
def with_retries(fn, *args, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (NoCredentialsError, PartialCredentialsError):
            raise SystemExit("❌ AWS credentials not found or incomplete. Run `aws configure`.")
        except (BotoCoreError, ClientError) as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"⚠️ Error: {e}. Retrying {attempt}/{MAX_RETRIES}...")
            time.sleep(RETRY_BACKOFF * attempt)

# =======================
# Random Generators
# =======================
def random_name():
    return ''.join(random.choices(string.ascii_uppercase, k=1)) + ''.join(random.choices(string.ascii_lowercase, k=random.randint(3,7)))

def random_email(name):
    return f"{name.lower()}{random.randint(1,9999)}@example.com"

def random_city():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=random.randint(5,10)))

def random_country():
    return ''.join(random.choices(string.ascii_uppercase, k=2))

def random_product():
    return {
        "product_id": f"P-{random.randint(1000,9999)}",
        "name": ''.join(random.choices(string.ascii_letters, k=random.randint(5,12))),
        "category": ''.join(random.choices(string.ascii_letters, k=random.randint(5,10))),
        "brand": ''.join(random.choices(string.ascii_letters, k=random.randint(3,8))),
        "base_price": round(random.uniform(10, 500), 2)
    }

def random_date_last_days(days=30):
    d = random.randint(0, days)
    secs = random.randint(0, 86399)
    return datetime.utcnow() - timedelta(days=d, seconds=secs)

# =======================
# Unified Schema
# =======================
# Every record will expose ALL these fields; non-applicable ones are None (-> null in JSON).
UNIFIED_FIELDS = [
    # housekeeping
    "record_type", "ingest_ts",
    # customer
    "customer_id", "name", "email", "country", "city", "created_at",
    # order
    "order_id", "order_date", "product_id", "product", "category", "brand",
    "quantity", "unit_price", "price", "discount_pct", "order_amount", "currency",
    # payment
    "payment_id", "payment_type", "amount", "status", "payment_date"
]

def base_record(record_type: str) -> dict:
    r = {k: None for k in UNIFIED_FIELDS}
    r["record_type"] = record_type
    r["ingest_ts"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return r

# =======================
# Data Builders
# =======================
def build_customer_pool(n):
    customers, now = [], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for _ in range(n):
        name = random_name()
        rec = base_record("customer")
        rec.update({
            "customer_id": str(uuid.uuid4()),
            "name": name,
            "email": random_email(name),
            "country": random_country(),
            "city": random_city(),
            "created_at": now,
        })
        customers.append(rec)
    return customers

def generate_orders_and_payments(customers, count, days_history):
    orders, payments = [], []
    for _ in range(count):
        cust = random.choice(customers)
        prod = random_product()
        qty  = random.randint(1, 5)

        unit_price = round(prod["base_price"] * random.uniform(0.9, 1.1), 2)
        discount   = random.choice([0, 5, 10, 15, 20])
        net        = round(unit_price * qty * (1 - discount/100.0), 2)

        o_id = f"ORD-{random.randint(100000,999999)}"
        o_dt = random_date_last_days(days_history).strftime("%Y-%m-%d %H:%M:%S")

        order_rec = base_record("order")
        order_rec.update({
            "customer_id": cust["customer_id"],
            "order_id": o_id,
            "order_date": o_dt,
            "product_id": prod["product_id"],
            "product": prod["name"],
            "category": prod["category"],
            "brand": prod["brand"],
            "quantity": qty,
            "unit_price": unit_price,
            # include 'price' explicitly as you requested (alias to unit_price for clarity)
            "price": unit_price,
            "discount_pct": discount,
            "order_amount": net,
            "currency": "USD",
        })
        orders.append(order_rec)

        pay_rec = base_record("payment")
        pay_rec.update({
            "order_id": o_id,
            "payment_id": f"PAY-{random.randint(100000,999999)}",
            "payment_type": random.choice(["card","upi","net_banking","cod"]),
            "amount": net,
            "currency": "USD",
            "status": random.choice(["success", "failed", "pending", "cancelled"]),
            "payment_date": o_dt,
        })
        payments.append(pay_rec)
    return orders, payments

# =======================
# S3 Upload (single file)
# =======================
def put_jsonl(s3, bucket, key, records):
    body = "".join(json.dumps(r) + "\n" for r in records).encode("utf-8")
    with_retries(s3.put_object, Bucket=bucket, Key=key, Body=body)

# =======================
# Main
# =======================
def main():
    print("🚀 Generating random E-commerce dataset → ONE JSONL file in S3 (with nulls for missing fields)")
    s3 = boto3.client("s3", region_name=AWS_REGION)

    customers = build_customer_pool(CUSTOMER_POOL_SIZE)
    orders, payments = generate_orders_and_payments(customers, ORDERS_COUNT, DAYS_HISTORY)

    # ONE list, ONE file
    unified = []
    unified.extend(customers)
    unified.extend(orders)
    unified.extend(payments)

    ts = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    key = f"{BASE_PREFIX}/ecommerce_unified_{ts}.jsonl"

    try:
        put_jsonl(s3, BUCKET_NAME, key, unified)
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return

    print(f"✅ Uploaded {len(unified)} records to s3://{BUCKET_NAME}/{key}")
    print(f"   - customers: {len(customers)}")
    print(f"   - orders:    {len(orders)}")
    print(f"   - payments:  {len(payments)}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e))
    except Exception as e:
        print(f"Unexpected error: {e}")
