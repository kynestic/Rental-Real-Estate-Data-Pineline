import os
import sys
import pika
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- 1. SỬA ĐƯỜNG DẪN (BẮT BUỘC) ---
# Đảm bảo PROJECT_ROOT trỏ đúng vào /opt/airflow (nơi chứa thư mục src)
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.rmq.consumer import consume_urls
from src.rmq.publisher import publish_urls
from src.load.loaders import load_raw
from src.extract import Muaban
from scripts.muaban import processed, parsed, served
from scripts.cleaning.clean_queue import purge_queues
from scripts.cleaning.clean_qdrant import reset_qdrant

SITE_NAME = 'muaban.net'
RMQ_HOST = 'host.docker.internal'

def delete_old_message(**kwargs):
    purge_queues(SITE_NAME)
    reset_qdrant()

# --- 3. CÁC HÀM WRAPPER ---

def run_publisher_task(**kwargs):
    print(f"[Publisher] Đang khởi động cho {SITE_NAME}...")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RMQ_HOST)
    )
    try:
        channel = connection.channel()
        # Gọi hàm đã import thành công ở trên
        publish_urls(Muaban, channel, SITE_NAME)
    finally:
        pass

def run_consumer_task(**kwargs):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RMQ_HOST)
    )
    try:
        channel = connection.channel()
        consume_urls(
            channel=channel, 
            crawler_tool=Muaban, 
            web_name=SITE_NAME, 
            load_raw=load_raw, 
            batch_size=200
        )
    finally:
        pass

# --- 4. ĐỊNH NGHĨA DAG ---
with DAG(
    'muaban_crawl_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    publish_task = PythonOperator(
        task_id='publish_urls_to_rmq',
        python_callable=run_publisher_task,
    )

    consume_task = PythonOperator(
        task_id='consume_and_crawl_data',
        python_callable=run_consumer_task,
    )

    # Các task parse và process...
    parse_task = PythonOperator(task_id='parse_data', python_callable=parsed.parse)
    process_task = PythonOperator(task_id='process_data', python_callable=processed.process)
    serve_task = PythonOperator(task_id='serve_data', python_callable=served.serve)
    queue_cleaning_task = PythonOperator(task_id='queue_cleaning', python_callable=delete_old_message)

    queue_cleaning_task >> [publish_task, consume_task] >> parse_task >> process_task >>serve_task 