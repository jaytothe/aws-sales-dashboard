import boto3
import json
import time

BUCKET = "sales-dashboard-jaytothe"
DATABASE = "sales_dashboard"

athena = boto3.client("athena", region_name="us-east-2")
s3 = boto3.client("s3", region_name="us-east-2")

def run_query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={
            "OutputLocation": "s3://sales-dashboard-jaytothe/athena-results/"
        }
    )
    qid = response["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ["FAILED", "CANCELLED"]:
            raise Exception(f"Query failed: {status}")
        time.sleep(1)
    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results["ResultSet"]["Rows"]
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    data = []
    for row in rows[1:]:
        data.append({headers[i]: col.get("VarCharValue", "") for i, col in enumerate(row["Data"])})
    return data

def lambda_handler(event, context):
    monthly = run_query("""
        SELECT SUBSTRING(date,1,7) AS month, ROUND(SUM(amount),2) AS total_revenue
        FROM sales_dashboard.processed
        GROUP BY SUBSTRING(date,1,7) ORDER BY month
    """)
    by_region = run_query("""
        SELECT region, ROUND(SUM(amount),2) AS total_revenue
        FROM sales_dashboard.processed
        GROUP BY region ORDER BY total_revenue DESC
    """)
    top_reps = run_query("""
        SELECT rep, COUNT(order_id) AS total_orders, ROUND(SUM(amount),2) AS total_revenue
        FROM sales_dashboard.processed
        GROUP BY rep ORDER BY total_revenue DESC
    """)
    by_product = run_query("""
        SELECT product, ROUND(SUM(amount),2) AS total_revenue
        FROM sales_dashboard.processed
        GROUP BY product ORDER BY total_revenue DESC
    """)
    total_revenue = run_query("""
        SELECT ROUND(SUM(amount),2) AS total FROM sales_dashboard.processed
    """)
    total_orders = run_query("""
        SELECT COUNT(order_id) AS total FROM sales_dashboard.processed
    """)

    data = {
        "monthly": monthly,
        "by_region": by_region,
        "top_reps": top_reps,
        "by_product": by_product,
        "total_revenue": total_revenue[0]["total"],
        "total_orders": total_orders[0]["total"]
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sales Dashboard 2024</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f0f2f5; color: #1a1a2e; }}
  header {{ background: #1a1a2e; color: white; padding: 1.5rem 2rem; display: flex; justify-content: space-between; align-items: center; }}
  header h1 {{ font-size: 1.3rem; font-weight: 500; letter-spacing: -0.02em; }}
  header p {{ font-size: 0.8rem; opacity: 0.5; margin-top: 3px; }}
  .tag {{ background: rgba(255,255,255,0.1); font-size: 11px; padding: 4px 10px; border-radius: 20px; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 1.5rem 2rem; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }}
  .kpi {{ background: white; border-radius: 10px; padding: 1.25rem 1.5rem; border: 1px solid #e8e8e8; }}
  .kpi label {{ font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.05em; display: block; margin-bottom: 8px; }}
  .kpi .value {{ font-size: 1.8rem; font-weight: 600; color: #1a1a2e; letter-spacing: -0.03em; }}
  .kpi .sub {{ font-size: 12px; color: #4f46e5; margin-top: 4px; }}
  .charts-top {{ display: grid; grid-template-columns: 1fr; gap: 1rem; margin-bottom: 1rem; }}
  .charts-bottom {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; }}
  .card {{ background: white; border-radius: 10px; padding: 1.5rem; border: 1px solid #e8e8e8; }}
  .card h2 {{ font-size: 12px; font-weight: 600; color: #999; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 1.25rem; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; color: #bbb; font-weight: 500; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; padding: 0 0 10px; }}
  td {{ padding: 10px 0; border-bottom: 1px solid #f5f5f5; color: #333; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tr:last-child td {{ border-bottom: none; }}
  .rank {{ width: 20px; color: #ccc; font-size: 12px; }}
  .bar-bg {{ background: #f0f2f5; border-radius: 4px; height: 6px; margin-top: 4px; }}
  .bar-fill {{ background: #4f46e5; border-radius: 4px; height: 6px; }}
  footer {{ text-align: center; padding: 2rem; font-size: 11px; color: #bbb; }}
</style>
</head>
<body>

<header>
  <div>
    <h1>Sales Dashboard</h1>
    <p>Automated pipeline — S3 + Lambda + Athena</p>
  </div>
  <span class="tag">2024 Full Year</span>
</header>

<div class="container">

  <div class="kpi-grid">
    <div class="kpi">
      <label>Total revenue</label>
      <div class="value">${float(data['total_revenue']):,.0f}</div>
      <div class="sub">Full year 2024</div>
    </div>
    <div class="kpi">
      <label>Total orders</label>
      <div class="value">{data['total_orders']}</div>
      <div class="sub">Across all regions</div>
    </div>
    <div class="kpi">
      <label>Avg order value</label>
      <div class="value">${float(data['total_revenue'])/int(data['total_orders']):,.0f}</div>
      <div class="sub">Per transaction</div>
    </div>
    <div class="kpi">
      <label>Active regions</label>
      <div class="value">{len(data['by_region'])}</div>
      <div class="sub">Geographic coverage</div>
    </div>
    <div class="kpi">
      <label>Top product</label>
      <div class="value" style="font-size:1.1rem;padding-top:6px;">{data['by_product'][0]['product']}</div>
      <div class="sub">${float(data['by_product'][0]['total_revenue']):,.0f} revenue</div>
    </div>
  </div>

  <div class="charts-top">
    <div class="card">
      <h2>Monthly revenue trend</h2>
      <canvas id="monthlyChart" height="60"></canvas>
    </div>
  </div>

  <div class="charts-bottom">
    <div class="card">
      <h2>Revenue by region</h2>
      <canvas id="regionChart" height="180"></canvas>
    </div>
    <div class="card">
      <h2>Revenue by product</h2>
      <canvas id="productChart" height="180"></canvas>
    </div>
    <div class="card">
      <h2>Rep leaderboard</h2>
      <table>
        <tr><th></th><th>Rep</th><th>Orders</th><th style="text-align:right">Revenue</th></tr>
        {"".join(f'''<tr>
          <td class="rank">{i+1}</td>
          <td>{r['rep']}</td>
          <td>{r['total_orders']}</td>
          <td class="num">${float(r['total_revenue']):,.0f}</td>
        </tr>''' for i, r in enumerate(data['top_reps']))}
      </table>
    </div>
  </div>

</div>

<footer>Built on AWS — S3 · Lambda · Athena · CloudWatch &nbsp;|&nbsp; Data refreshes automatically on upload</footer>

<script>
const monthly = {json.dumps(monthly)};
const regions = {json.dumps(by_region)};
const products = {json.dumps(by_product)};

const purple = '#4f46e5';
const purples = ['#4f46e5','#6366f1','#818cf8','#a5b4fc','#c7d2fe'];

new Chart(document.getElementById('monthlyChart'), {{
  type: 'line',
  data: {{
    labels: monthly.map(d => d.month),
    datasets: [{{
      data: monthly.map(d => parseFloat(d.total_revenue)),
      borderColor: purple,
      backgroundColor: 'rgba(79,70,229,0.06)',
      fill: true, tension: 0.4, pointRadius: 5,
      pointBackgroundColor: purple, borderWidth: 2
    }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ grid: {{ color: '#f5f5f5' }}, ticks: {{ callback: v => '$' + v.toLocaleString(), font: {{ size: 11 }} }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 11 }} }} }}
    }}
  }}
}});

new Chart(document.getElementById('regionChart'), {{
  type: 'bar',
  data: {{
    labels: regions.map(d => d.region),
    datasets: [{{ data: regions.map(d => parseFloat(d.total_revenue)), backgroundColor: purples, borderRadius: 4 }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ grid: {{ color: '#f5f5f5' }}, ticks: {{ callback: v => '$' + v.toLocaleString(), font: {{ size: 11 }} }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 11 }} }} }}
    }}
  }}
}});

new Chart(document.getElementById('productChart'), {{
  type: 'doughnut',
  data: {{
    labels: products.map(d => d.product),
    datasets: [{{ data: products.map(d => parseFloat(d.total_revenue)), backgroundColor: purples, borderWidth: 0 }}]
  }},
  options: {{
    plugins: {{
      legend: {{ position: 'bottom', labels: {{ font: {{ size: 11 }}, padding: 16 }} }}
    }},
    cutout: '65%'
  }}
}});
</script>
</body>
</html>"""

    s3.put_object(
        Bucket=BUCKET,
        Key="dashboard/index.html",
        Body=html,
        ContentType="text/html"
    )

    return {
        "statusCode": 200,
        "body": "Dashboard updated successfully"
    }
