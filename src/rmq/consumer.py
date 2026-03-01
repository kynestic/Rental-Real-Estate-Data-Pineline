import uuid
import json, time
import pika
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from src.load.loaders import load_raw
import threading
from src.quality.pipeline_monitor import start_monitor

# Load biến môi trường
load_dotenv()

def create_task_payload(url, site, correlation_id, task_type, page_num, current_try=1, status_code = None, error_message_list = None):
    return {
        "task_id": str(uuid.uuid4()),
        "correlation_id": correlation_id,
        "task_type": task_type,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "context": {
            "site": site,
            "url": url,
            "page_source": page_num,
            "method": "GET",
            "headers": {}
        },
        "retry_policy": {
            "current_try": current_try,
            "max_tries": 3,
            "history_status_codes": status_code,
            "error_message": error_message_list
        }
    }

def publish_item(channel, queue, item, persistent=True):
    """Hàm gửi tin nhắn an toàn"""
    body = json.dumps(item).encode('utf-8')
    props = pika.BasicProperties(delivery_mode=2) if persistent else None
    
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=body,
        properties=props,
        mandatory=True
    )
    return True
    


def consume_urls(channel, crawler_tool, web_name, load_raw, batch_size = 200):
    """
    Main Loop Consumer
    :param crawler_tool: Object có hàm getDataList(url) -> list items
    :param save_tool: Object có client và hàm send_raw_to_minio
    :param send_to_dlq_func: Hàm gửi lỗi (channel, msg, status, queue_name, dlq_name, content)
    """
    queue_url = web_name + "_url" 
    queue_html = web_name + "_html"
    queue_dlq = web_name + "_dlq"
    queue_retry = web_name + "_retry"
    
    print('@@@@@ Khởi chạy Monitor chạy ngầm @@@@@')
    # Tạo một luồng riêng để chạy monitor
    monitor_thread = threading.Thread(
        target=start_monitor, 
        args=(web_name, 1800 * 20),
        daemon=True # Quan trọng: Thread này sẽ tự đóng khi task chính kết thúc
    )
    monitor_thread.start()

    retry_args = {
        'x-message-ttl': 30000,              # 1. TTL: Tin nhắn sống 30 giây (30000 ms)
        'x-dead-letter-exchange': '',        # 2. Sau khi chết, gửi lại exchange mặc định
        'x-dead-letter-routing-key': queue_url # 3. Đẩy lại về hàng đợi chính (để xử lý lại)
    }

    # Khai báo queue
    channel.queue_declare(queue=queue_dlq)
    channel.queue_declare(queue=queue_url)
    channel.queue_declare(queue=queue_html)
    channel.queue_declare(queue=queue_retry, durable=True, arguments=retry_args)

    # Config cho queue retry
    channel.basic_qos(prefetch_count=batch_size)
    executor = ThreadPoolExecutor(max_workers=10)
    batch_buffer = []
    last_activity = time.time()
    for method, properties, body in channel.consume(queue=queue_url, inactivity_timeout=180):
        if method:
            batch_buffer.append({
                'properties': properties,
                'body': json.loads(body.decode('utf-8')),
                'delivery_tag': method.delivery_tag
            })
            last_activity = time.time()
        
        is_buffer_full = len(batch_buffer) >= batch_size
        is_timeout = (method is None and len(batch_buffer) > 0)
        url_delivery_tag_map = {}
        url_payload_map = {}
        origin = None
        comparison = ''
        success_batch = []
 
        if method is None and not batch_buffer:
            if time.time() - last_activity > 600: 
                print("Hàng đợi trống trong 10 phút. Đang dừng consumer...")
                break

        if is_buffer_full or is_timeout:
            url_list = []
            if (len(batch_buffer) == 0):
                continue
            for item in batch_buffer:
                url_item = item['body']['context']['url']
                url_list.append(url_item)
                url_delivery_tag_map[url_item] = item['delivery_tag']
                url_payload_map[url_item] = item['body']
                

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_map = {executor.submit(crawler_tool.getData, url): url for url in url_list}

                for f in as_completed(future_map):
                    url = future_map[f]
                    html, status, error_msg = f.result()
                    if status == 200:
                        success_batch.append({
                            'correlation_id': url_payload_map[url]['correlation_id'],
                            'url': url,
                            'status': status,
                            'html': html,
                            
                        })
                        continue

                    if url_payload_map[url]['retry_policy']['current_try'] >= url_payload_map[url]['retry_policy']['max_tries']:
                        channel.basic_publish(
                            exchange='',
                            routing_key=queue_dlq,
                            body=json.dumps(retry_payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Persistent message
                            )
                        )
                        channel.basic_ack(delivery_tag = url_delivery_tag_map[url])
                        continue

                    if status == 500 or status == 502 or status == 503 or status == 504 or error_msg:
                        history_status_code = []
                        error_message = []
                        if url_payload_map[url]['retry_policy']["history_status_codes"]:
                            history_status_code = url_payload_map[url]['retry_policy']["history_status_codes"]
                        if url_payload_map[url]['retry_policy']['error_message']:
                            error_message = url_payload_map[url]['retry_policy']['error_message']

                        retry_payload = create_task_payload(
                            url=url, 
                            site=url_payload_map[url]["context"]['site'], 
                            task_type='CRAWL_DATA', 
                            correlation_id=url_payload_map[url]['correlation_id'],
                            page_num="",
                            current_try = url_payload_map[url]['retry_policy']['current_try'] + 1,
                            status_code = history_status_code.append(status),
                            error_message_list=error_message.append(error_msg)
                        )
                        print(retry_payload)
                        channel.basic_publish(
                            exchange='',
                            routing_key=f'{web_name}_retry_queue', # <-- Gửi vào queue trung gian
                            body=json.dumps(retry_payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Persistent message
                            )
                        )
                        channel.basic_ack(delivery_tag = url_delivery_tag_map[url])

                    if status == 429 or status == 403 or status == 400 or status == 404 or status == 410 or status == 401:
                        if status == 429:
                            time.sleep(10)
                        history_status_code = []
                        error_message = []
                        if url_payload_map[url]['retry_policy']["history_status_codes"]:
                            history_status_code = url_payload_map[url]['retry_policy']["history_status_codes"]
                        if url_payload_map[url]['retry_policy']['error_message']:
                            error_message = url_payload_map[url]['retry_policy']['error_message']

                        dlq_payload = create_task_payload(
                            url=url, 
                            site=url_payload_map[url]["context"]['site'], 
                            task_type='CRAWL_DATA', 
                            correlation_id=url_payload_map[url]['correlation_id'],
                            page_num="",
                            current_try = url_payload_map[url]['retry_policy']['current_try'],
                            status_code = history_status_code.append(status),
                            error_message_list=error_message.append(error_msg)
                        )

                        channel.basic_publish(
                            exchange='',
                            routing_key=queue_dlq, # <-- Gửi vào queue trung gian
                            body=json.dumps(dlq_payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Persistent message
                            )
                        )

                        channel.basic_ack(delivery_tag = url_delivery_tag_map[url])
            
            batch_buffer = []
            if success_batch:
                # mem_file = io.BytesIO()
                # with gzip.GzipFile(fileobj=mem_file, mode='wb') as gz:
                #     for item in success_batch:
                #         # item['body'] chứa dữ liệu (url, html...), item['tag'] là delivery_tag
                #         # ensure_ascii=False để giữ tiếng Việt
                #         line = json.dumps(item, ensure_ascii=False) + "\n"
                #         gz.write(line.encode('utf-8'))
                # mem_file.seek(0)
                # now = datetime.utcnow()
                # year, month, day = now.year, now.month, now.day
                # file_path = f"raw/source=batdongsan/year={now.year}/month={now.month:02d}/day={now.day:02d}/batch_{int(time.time())}.json.gz"
                
                check_comparison = ''
                for item in success_batch:
                    check_comparison = check_comparison + item['url']
                
                if check_comparison == comparison and comparison != '':
                    channel.basic_ack(delivery_tag=max(list(url_delivery_tag_map.values())), multiple=True)
                    break
                else:
                    comparison = check_comparison

                file_path = load_raw(success_batch, site=url_payload_map[url]["context"]['site'])

                # client.put_object(
                #     bucket_name='data-lake',
                #     object_name=file_path,
                #     data=mem_file,
                #     length=mem_file.getbuffer().nbytes,
                #     content_type="application/json"
                # )

                


                channel.basic_ack(delivery_tag=max(list(url_delivery_tag_map.values())), multiple=True)
                for item in success_batch:
                    channel.basic_publish(
                        exchange='',
                        routing_key=queue_html, # <-- Gửi vào queue trung gian
                        body=json.dumps({
                            'url': item['url'],
                            'path': file_path,
                            'correlation_id': item['correlation_id']
                        }),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent message
                        )
                    )
                last_activity = time.time()


        

