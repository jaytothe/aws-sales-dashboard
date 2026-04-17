import boto3
import csv
from io import StringIO

REQUIRED_COLUMNS = {"order_id", "date", "region", "product", "amount", "rep"}
s3 = boto3.client("s3")

def lambda_handler(event, context):
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = record["object"]["key"]

    if not key.startswith("raw/") or not key.endswith(".csv"):
        return {"statusCode": 200, "body": "Skipped — not a raw CSV"}

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(content))
    columns = set(reader.fieldnames or [])

    if not REQUIRED_COLUMNS.issubset(columns):
        missing = REQUIRED_COLUMNS - columns
        raise ValueError(f"CSV missing required columns: {missing}")

    rows = list(reader)
    if len(rows) == 0:
        raise ValueError("CSV is empty")

    filename = key.split("/")[-1]
    new_key = f"processed/{filename}"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=new_key
    )

    print(f"Validated {len(rows)} rows. Copied to {new_key}")
    return {
        "statusCode": 200,
        "body": f"Processed {len(rows)} rows → {new_key}"
    }
