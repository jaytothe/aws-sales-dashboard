import csv
import random
import boto3
import os
from datetime import datetime, timedelta
from io import StringIO

BUCKET = "sales-dashboard-jaytothe"
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
PRODUCTS = ["Pro Plan", "Basic Plan", "Enterprise Plan", "Add-on Support", "Consulting"]
REPS = ["Marcus Hill", "Tanya Brooks", "Devon Carter", "Priya Nair", "James Wu", "Sofia Reyes", "Andre Lewis"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def lambda_handler(event, context):
    rows = []
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)

    for i in range(1, 501):
        rows.append({
            "order_id": f"ORD-{i:04d}",
            "date": random_date(start, end).strftime("%Y-%m-%d"),
            "region": random.choice(REGIONS),
            "product": random.choice(PRODUCTS),
            "amount": round(random.uniform(500, 15000), 2),
            "rep": random.choice(REPS)
        })

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key="raw/sales_2024.csv",
        Body=output.getvalue()
    )

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(rows)} rows to s3://{BUCKET}/raw/sales_2024.csv"
    }
