from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import random
import uuid

# Configuration - UPDATE THESE WITH YOUR PROJECT DETAILS
PROJECT_ID = 'aj-sales-analytics-2024'
DATASET_ID = 'sales_analytics'
BUCKET_NAME = 'aj-raw-data-20241125'

# DAG configuration
default_args = {
    'owner': 'test-user',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'test_sales_data_generator',
    default_args=default_args,
    description='Simple test DAG to generate sales data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'sales', 'demo'],
)

def generate_test_sales_data(**context):
    """Generate simple test sales data that you can see change"""
    
    print("🛍️ Generating test sales data...")
    
    # Simple product list
    products = [
        {'name': 'Test Laptop', 'category': 'Electronics', 'price': 999.99},
        {'name': 'Test Phone', 'category': 'Electronics', 'price': 599.99},
        {'name': 'Test Shirt', 'category': 'Clothing', 'price': 29.99},
        {'name': 'Test Jeans', 'category': 'Clothing', 'price': 79.99},
        {'name': 'Test Coffee Maker', 'category': 'Home', 'price': 149.99},
    ]
    
    locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    
    # Generate 20 test transactions
    data = []
    current_time = datetime.now()
    
    for i in range(20):
        product = random.choice(products)
        quantity = random.randint(1, 3)
        total_amount = round(product['price'] * quantity, 2)
        
        # Create transaction within last hour
        transaction_time = current_time - timedelta(minutes=random.randint(0, 60))
        
        data.append({
            'transaction_id': f"TEST-{str(uuid.uuid4())[:8]}",
            'transaction_date': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': f"TEST-{product['category'][:3].upper()}-{i:03d}",
            'product_name': product['name'],
            'category': product['category'],
            'quantity': quantity,
            'unit_price': product['price'],
            'total_amount': total_amount,
            'customer_id': f"TEST-CUST-{random.randint(1000, 9999)}",
            'store_location': random.choice(locations)
        })
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    filename = f"/tmp/test_sales_{context['ds_nodash']}_{context['ts_nodash']}.csv"
    df.to_csv(filename, index=False)
    
    # Print summary for verification
    total_revenue = df['total_amount'].sum()
    print(f"✅ Generated {len(data)} test transactions")
    print(f"💰 Total revenue: ${total_revenue:,.2f}")
    print(f"📦 Products: {df['product_name'].nunique()} unique items")
    print(f"🏪 Locations: {', '.join(df['store_location'].unique())}")
    print(f"📁 Saved to: {filename}")
    
    return filename

def verify_bigquery_data(**context):
    """Verify data was loaded to BigQuery"""
    from google.cloud import bigquery
    
    print("🔍 Verifying data in BigQuery...")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Check total row count
    query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN DATE(transaction_date) = CURRENT_DATE() THEN 1 END) as today_records,
        MAX(transaction_date) as latest_transaction,
        SUM(total_amount) as total_revenue
    FROM `{PROJECT_ID}.{DATASET_ID}.raw_sales_data`
    """
    
    try:
        result = client.query(query).to_dataframe()
        
        if len(result) > 0:
            row = result.iloc[0]
            print(f"✅ BigQuery Verification:")
            print(f"📊 Total records: {row['total_records']}")
            print(f"📅 Today's records: {row['today_records']}")
            print(f"💰 Total revenue: ${row['total_revenue']:,.2f}")
            print(f"🕐 Latest transaction: {row['latest_transaction']}")
            
            if row['today_records'] > 0:
                print("🎉 SUCCESS: New data found in BigQuery!")
            else:
                print("⚠️ WARNING: No data from today found")
        else:
            print("❌ ERROR: No data found in BigQuery")
            
    except Exception as e:
        print(f"❌ BigQuery verification failed: {e}")

# Task 1: Generate test sales data
generate_data = PythonOperator(
    task_id='generate_test_sales_data',
    python_callable=generate_test_sales_data,
    dag=dag,
)

# Task 2: Upload to GCS
upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src="{{ task_instance.xcom_pull(task_ids='generate_test_sales_data') }}",
    dst="test_data/{{ ds }}/test_sales_{{ ts_nodash }}.csv",
    bucket=BUCKET_NAME,
    dag=dag,
)

# Task 3: Load to BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=BUCKET_NAME,
    source_objects=["test_data/{{ ds }}/test_sales_{{ ts_nodash }}.csv"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.raw_sales_data",
    schema_fields=[
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_date", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "unit_price", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "total_amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "store_location", "type": "STRING", "mode": "NULLABLE"},
    ],
    write_disposition='WRITE_APPEND',  # Add to existing data
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

# Task 4: Verify data loaded successfully
verify_data = PythonOperator(
    task_id='verify_bigquery_data',
    python_callable=verify_bigquery_data,
    dag=dag,
)

# Set task dependencies
generate_data >> upload_to_gcs >> load_to_bigquery >> verify_data
