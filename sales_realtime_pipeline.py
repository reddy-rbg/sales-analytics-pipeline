from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import random
import uuid
from google.cloud import bigquery

# Configuration
PROJECT_ID = 'aj-sales-analytics-2024'
DATASET_ID = 'sales_analytics'
BUCKET_RAW = 'aj-raw-data-20241125'
LOCATION = 'us-central1'

# Default arguments
default_args = {
    'owner': 'sales-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@yourcompany.com'],  # Replace with your email
}

# Create DAG
dag = DAG(
    'sales_realtime_pipeline_composer',
    default_args=default_args,
    description='Real-time sales pipeline on Google Cloud Composer',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'analytics', 'composer', 'bigquery'],
)

def generate_realistic_sales_data(**context):
    """Generate realistic sales data with seasonal patterns"""
    
    print(f"🛍️ Generating sales data for {context['ds']}")
    
    # Enhanced product catalog with realistic pricing
    products_catalog = {
        'Electronics': [
            {'name': 'MacBook Pro M3', 'base_price': 1999, 'variance': 0.1},
            {'name': 'iPhone 15 Pro', 'base_price': 999, 'variance': 0.05},
            {'name': 'iPad Air', 'base_price': 599, 'variance': 0.08},
            {'name': 'AirPods Pro', 'base_price': 249, 'variance': 0.12},
            {'name': 'Apple Watch Series 9', 'base_price': 399, 'variance': 0.10},
            {'name': 'Samsung Galaxy S24', 'base_price': 899, 'variance': 0.07},
            {'name': 'Dell XPS 13', 'base_price': 1199, 'variance': 0.15},
        ],
        'Clothing': [
            {'name': 'Nike Air Max', 'base_price': 120, 'variance': 0.20},
            {'name': 'Levi\'s 501 Jeans', 'base_price': 80, 'variance': 0.15},
            {'name': 'Patagonia Jacket', 'base_price': 200, 'variance': 0.10},
            {'name': 'Adidas Hoodie', 'base_price': 65, 'variance': 0.18},
            {'name': 'Ray-Ban Sunglasses', 'base_price': 150, 'variance': 0.12},
            {'name': 'North Face Backpack', 'base_price': 89, 'variance': 0.14},
        ],
        'Home': [
            {'name': 'Dyson V15 Vacuum', 'base_price': 449, 'variance': 0.08},
            {'name': 'Ninja Blender', 'base_price': 129, 'variance': 0.15},
            {'name': 'Instant Pot Duo', 'base_price': 99, 'variance': 0.20},
            {'name': 'Philips Air Fryer', 'base_price': 159, 'variance': 0.12},
            {'name': 'Roomba i7', 'base_price': 599, 'variance': 0.10},
            {'name': 'KitchenAid Mixer', 'base_price': 349, 'variance': 0.08},
        ]
    }
    
    # Geographic locations with different market characteristics
    locations = [
        {'city': 'New York', 'premium_factor': 1.15, 'volume_factor': 1.3},
        {'city': 'Los Angeles', 'premium_factor': 1.10, 'volume_factor': 1.2},
        {'city': 'Chicago', 'premium_factor': 1.05, 'volume_factor': 1.1},
        {'city': 'Houston', 'premium_factor': 1.02, 'volume_factor': 1.0},
        {'city': 'Phoenix', 'premium_factor': 0.98, 'volume_factor': 0.9},
        {'city': 'Philadelphia', 'premium_factor': 1.08, 'volume_factor': 1.05},
    ]
    
    # Time-based factors
    current_hour = datetime.now().hour
    day_of_week = datetime.now().weekday()
    
    # Shopping patterns (higher sales during certain hours)
    hour_factors = {
        range(6, 9): 0.7,    # Early morning
        range(9, 12): 1.2,   # Morning peak
        range(12, 15): 1.4,  # Lunch peak
        range(15, 18): 1.1,  # Afternoon
        range(18, 21): 1.5,  # Evening peak
        range(21, 24): 0.8,  # Late evening
        range(0, 6): 0.3,    # Night
    }
    
    hour_multiplier = 1.0
    for hour_range, factor in hour_factors.items():
        if current_hour in hour_range:
            hour_multiplier = factor
            break
    
    # Weekend boost
    weekend_multiplier = 1.3 if day_of_week in [5, 6] else 1.0
    
    # Generate realistic number of transactions
    base_transactions = 75
    transaction_count = int(base_transactions * hour_multiplier * weekend_multiplier * random.uniform(0.8, 1.2))
    
    sales_data = []
    current_time = datetime.now()
    
    for i in range(transaction_count):
        # Select category with realistic distribution
        category_weights = {'Electronics': 0.4, 'Clothing': 0.35, 'Home': 0.25}
        category = random.choices(list(category_weights.keys()), 
                                weights=list(category_weights.values()))[0]
        
        # Select product
        product_info = random.choice(products_catalog[category])
        product_name = product_info['name']
        base_price = product_info['base_price']
        price_variance = product_info['variance']
        
        # Select location
        location_info = random.choice(locations)
        store_location = location_info['city']
        
        # Calculate realistic pricing
        price_variation = random.uniform(1 - price_variance, 1 + price_variance)
        location_adjustment = location_info['premium_factor']
        unit_price = round(base_price * price_variation * location_adjustment, 2)
        
        # Realistic quantity distribution
        quantity_weights = [60, 25, 10, 4, 1]  # Heavily weighted toward 1-2 items
        quantity = random.choices([1, 2, 3, 4, 5], weights=quantity_weights)[0]
        
        total_amount = round(unit_price * quantity, 2)
        
        # Generate transaction time within the last hour
        minutes_ago = random.randint(0, 59)
        transaction_time = current_time - timedelta(minutes=minutes_ago)
        
        # Generate realistic customer ID (repeat customers)
        customer_id = f"CUST-{random.randint(10000, 89999)}"
        
        # Product ID based on category and name
        product_id = f"{category[:3].upper()}-{abs(hash(product_name)) % 10000:04d}"
        
        sales_data.append({
            'transaction_id': str(uuid.uuid4()),
            'transaction_date': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'customer_id': customer_id,
            'store_location': store_location,
        })
    
    # Save to CSV
    df = pd.DataFrame(sales_data)
    filename = f"/tmp/sales_data_{context['ds_nodash']}_{context['ts_nodash']}.csv"
    df.to_csv(filename, index=False)
    
    print(f"✅ Generated {len(sales_data)} transactions (${df['total_amount'].sum():,.2f} total)")
    print(f"📊 Average order value: ${df['total_amount'].mean():.2f}")
    print(f"🏆 Top category: {df.groupby('category')['total_amount'].sum().idxmax()}")
    
    return filename

def generate_smart_predictions(**context):
    """Generate ML predictions based on recent sales patterns"""
    
    print("🔮 Generating smart sales predictions...")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Advanced query to get sales patterns
    analysis_query = f"""
    WITH sales_patterns AS (
        SELECT 
            product_id,
            product_name,
            category,
            store_location,
            AVG(total_amount) as avg_sales,
            STDDEV(total_amount) as sales_stddev,
            COUNT(*) as transaction_count,
            MAX(transaction_date) as last_sale,
            -- Weekly pattern analysis
            AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM transaction_date) IN (1,7) THEN total_amount END) as weekend_avg,
            AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM transaction_date) BETWEEN 2 AND 6 THEN total_amount END) as weekday_avg,
            -- Time trend
            CORR(UNIX_SECONDS(transaction_date), total_amount) as time_correlation
        FROM `{PROJECT_ID}.{DATASET_ID}.raw_sales_data`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        GROUP BY product_id, product_name, category, store_location
        HAVING transaction_count >= 3
        ORDER BY avg_sales DESC
        LIMIT 30
    )
    SELECT * FROM sales_patterns
    """
    
    try:
        sales_patterns = client.query(analysis_query).to_dataframe()
        
        if len(sales_patterns) > 0:
            predictions = []
            current_date = datetime.now().date()
            
            for _, product in sales_patterns.iterrows():
                base_prediction = product['avg_sales']
                volatility = product['sales_stddev'] if pd.notna(product['sales_stddev']) else base_prediction * 0.2
                
                # Weekend vs weekday pattern
                weekend_boost = 1.0
                if pd.notna(product['weekend_avg']) and pd.notna(product['weekday_avg']):
                    if product['weekend_avg'] > product['weekday_avg']:
                        weekend_boost = product['weekend_avg'] / product['weekday_avg']
                
                # Trend factor
                trend_factor = 1.0
                if pd.notna(product['time_correlation']):
                    trend_factor = 1.0 + (product['time_correlation'] * 0.1)  # Modest trend influence
                
                # Generate 7-day predictions
                for days_ahead in range(1, 8):
                    future_date = current_date + timedelta(days=days_ahead)
                    
                    # Apply day-of-week pattern
                    day_factor = weekend_boost if future_date.weekday() in [5, 6] else 1.0
                    
                    # Seasonal variation
                    seasonal_factor = 1.0 + (0.05 * random.sin(days_ahead * 0.5))
                    
                    # Market volatility
                    volatility_factor = random.uniform(0.9, 1.1)
                    
                    # Calculate prediction
                    predicted_sales = (base_prediction * 
                                     trend_factor * 
                                     day_factor * 
                                     seasonal_factor * 
                                     volatility_factor)
                    
                    predicted_sales = max(0, round(predicted_sales, 2))
                    
                    # Confidence intervals based on historical volatility
                    confidence_lower = max(0, round(predicted_sales - (volatility * 0.8), 2))
                    confidence_upper = round(predicted_sales + (volatility * 0.8), 2)
                    
                    predictions.append({
                        'prediction_id': f"{product['product_id']}_{future_date.strftime('%Y%m%d')}",
                        'prediction_date': future_date.strftime('%Y-%m-%d'),
                        'product_id': product['product_id'],
                        'product_name': product['product_name'],
                        'category': product['category'],
                        'store_location': product['store_location'],
                        'predicted_sales': predicted_sales,
                        'confidence_interval_lower': confidence_lower,
                        'confidence_interval_upper': confidence_upper,
                        'model_version': f"smart_v_{context['ds_nodash']}",
                        'prediction_confidence': min(0.95, 0.7 + (product['transaction_count'] * 0.02))
                    })
            
            # Save predictions
            pred_df = pd.DataFrame(predictions)
            pred_filename = f"/tmp/predictions_{context['ds_nodash']}.csv"
            pred_df.to_csv(pred_filename, index=False)
            
            print(f"✅ Generated {len(predictions)} smart predictions")
            print(f"📈 Average prediction: ${pred_df['predicted_sales'].mean():.2f}")
            
            return pred_filename
        else:
            print("⚠️ No sufficient data for predictions")
            return None
            
    except Exception as e:
        print(f"❌ Error generating predictions: {e}")
        return None

# Task 1: Generate realistic sales data
generate_sales_task = PythonOperator(
    task_id='generate_realistic_sales',
    python_callable=generate_realistic_sales_data,
    dag=dag,
)

# Task 2: Upload sales data to GCS
upload_sales_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_sales_to_gcs',
    src="{{ task_instance.xcom_pull(task_ids='generate_realistic_sales') }}",
    dst="hourly_sales/{{ ds }}/sales_{{ ts_nodash }}.csv",
    bucket=BUCKET_RAW,
    dag=dag,
)

# Task 3: Load sales data to BigQuery
load_sales_bq = GCSToBigQueryOperator(
    task_id='load_sales_to_bigquery',
    bucket=BUCKET_RAW,
    source_objects=["hourly_sales/{{ ds }}/sales_{{ ts_nodash }}.csv"],
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
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

# Task 4: Generate smart predictions (daily)
generate_predictions_task = PythonOperator(
    task_id='generate_smart_predictions',
    python_callable=generate_smart_predictions,
    dag=dag,
)

# Task 5: Upload predictions to GCS
upload_predictions_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_predictions_to_gcs',
    src="{{ task_instance.xcom_pull(task_ids='generate_smart_predictions') }}",
    dst="predictions/{{ ds }}/predictions_{{ ts_nodash }}.csv",
    bucket=BUCKET_RAW,
    dag=dag,
)

# Task 6: Load predictions to BigQuery
load_predictions_bq = GCSToBigQueryOperator(
    task_id='load_predictions_to_bigquery',
    bucket=BUCKET_RAW,
    source_objects=["predictions/{{ ds }}/predictions_{{ ts_nodash }}.csv"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.sales_predictions",
    schema_fields=[
        {"name": "prediction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "prediction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "store_location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "predicted_sales", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "confidence_interval_lower", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "confidence_interval_upper", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "model_version", "type": "STRING", "mode": "NULLABLE"},
        {"name": "prediction_confidence", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    write_disposition='WRITE_TRUNCATE',  # Replace predictions each run
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

# Define task dependencies
generate_sales_task >> upload_sales_gcs >> load_sales_bq
generate_sales_task >> generate_predictions_task >> upload_predictions_gcs >> load_predictions_bq