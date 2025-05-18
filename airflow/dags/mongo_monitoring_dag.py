from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import sys

# Add project directory to path
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Database monitoring DAG (daily)
with DAG(
    'mongo_monitoring',
    default_args=default_args,
    description='Monitor database health and performance',
    schedule_interval='10 1 * * *',  # Daily at 01:10 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
       
    def check_mongodb_health():
        """Check MongoDB health"""
        try:
            client = MongoClient(
                config.mongo_host,
                config.mongo_port,
                username=config.user,
                password=config.password
            )
            db = client[config.mongo_db]
            
            # Get collection stats
            stats = db.command("collStats", config.mongo_collection)
            
            # Get document count
            doc_count = db[config.mongo_collection].count_documents({})
            
            # Log results
            with open('/home/nicholas/Documents/IOT_Weather/db_logs/db_health.log', 'a') as f:
                f.write(f"\n--- MongoDB Health Check ({datetime.now()}) ---\n")
                f.write(f"Collection size: {stats.get('size')/1024/1024:.2f} MB\n")
                f.write(f"Storage size: {stats.get('storageSize')/1024/1024:.2f} MB\n")
                f.write(f"Document count: {doc_count}\n")
                
            client.close()
            return "MongoDB health check completed"
        except Exception as e:
            return f"MongoDB health check failed: {e}"


    mongodb_task = PythonOperator(
        task_id='check_mongodb_health',
        python_callable=check_mongodb_health,
    )