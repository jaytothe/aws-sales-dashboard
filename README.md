# AWS Automated Sales Dashboard

A serverless data pipeline that automatically generates a live sales analytics dashboard when new data is uploaded to S3.

![Dashboard](dashboard)

## Live Demo
[View Dashboard](https://sales-dashboard-jaytothe.s3.us-east-2.amazonaws.com/dashboard/index.html)

## Architecture
## AWS Services Used

- **S3** — Raw data ingestion, processed storage, static dashboard hosting
- **Lambda** — Data validation, ETL orchestration, dashboard generation
- **Athena** — Serverless SQL queries on S3 data
- **CloudWatch** — Error monitoring and alerting
- **SNS** — Email notifications on pipeline failures
- **IAM** — Least-privilege roles for each service

## How It Works

1. Drop a CSV file into the `raw/` S3 prefix
2. S3 event triggers `ProcessSalesData` Lambda automatically
3. Lambda validates the schema and moves the file to `processed/`
4. `SalesDashboard` Lambda queries Athena and regenerates the HTML dashboard
5. Dashboard is instantly available at the public S3 URL

## CSV Format

```csv
order_id,date,region,product,amount,rep
ORD-0001,2024-01-15,Northeast,Pro Plan,4250.00,Marcus Hill
```

## Setup

1. Create S3 bucket with `raw/`, `processed/`, `curated/`, `athena-results/`, `dashboard/` prefixes
2. Create IAM roles using policies in `iam/`
3. Deploy Lambda functions from `lambda/`
4. Create Athena database and table pointing at `processed/`
5. Set up CloudWatch alarms for each Lambda error metric

## What I Learned

Building this project taught me how AWS services connect in a real event-driven architecture. The biggest insight was how S3 event notifications, Lambda, and Athena can chain together to create a fully automated pipeline with zero manual steps between data upload and dashboard refresh.
