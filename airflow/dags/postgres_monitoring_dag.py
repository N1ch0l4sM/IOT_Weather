from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
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
    'psotgres_monitoring',
    default_args=default_args,
    description='Monitor database health and performance',
    schedule_interval='10 1 * * *',  # Daily at 01:10 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    def check_postgres_health():
        """Check PostgreSQL database health"""
        try:
            conn = psycopg2.connect(
                dbname=config.postG_db,
                user=config.user,
                password=config.password,
                host=config.postG_host,
                port=config.postG_port
            )
            cursor = conn.cursor()
            
            cursor.execute("ANALYZE;")
            
            # Check database size
            cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            db_size = cursor.fetchone()[0]
            
            # Check table sizes and counts
            cursor.execute("""
                SELECT 
                    relname as table_name,
                    pg_size_pretty(pg_total_relation_size(relid)) as table_size,
                    n_live_tup as row_count
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(relid) DESC;
            """)
            table_stats = cursor.fetchall()
            
            # Log results
            with open('/home/nicholas/Documents/IOT_Weather/db_logs/db_health.log', 'a') as f:
                f.write(f"\n--- PostgreSQL Health Check ({datetime.now()}) ---\n")
                f.write(f"Database Size: {db_size}\n")
                f.write("Table Statistics:\n")
                for table in table_stats:
                    f.write(f"  - {table[0]}: Size={table[1]}, Rows={table[2]}\n")
                
            cursor.close()
            conn.close()
            return "PostgreSQL health check completed"
        except Exception as e:
            return f"PostgreSQL health check failed: {e}"
    
    postgres_task = PythonOperator(
        task_id='check_postgres_health',
        python_callable=check_postgres_health,
    )