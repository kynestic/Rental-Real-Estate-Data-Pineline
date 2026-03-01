import pika
import uuid
import json
import time
from datetime import datetime
from src.config.logger import get_logger, ContextAdapter 
base_logger = get_logger("ProducerBDS")
import hashlib
import os



def get_url_hash(url):
    """Tạo mã hash MD5 từ URL để làm key định danh"""
    return hashlib.md5(url.strip().encode('utf-8')).hexdigest()

def check_and_add_url(url_list, site):
    storage_file = f'tmp/crawled_hashes_{site}.txt'
    os.makedirs('tmp', exist_ok=True) # Đảm bảo thư mục tmp tồn tại
    
    # 1. Đọc tất cả hash đã có vào SET (chỉ đọc 1 lần)
    crawled_hashes = set()
    if os.path.exists(storage_file):
        with open(storage_file, 'r', encoding='utf-8') as f:
            crawled_hashes = {line.strip() for line in f}

    new_urls = []
    hashes_to_add = []

    # 2. Kiểm tra nhanh toàn bộ list URL truyền vào
    for url in url_list:
        url_hash = get_url_hash(url)
        
        if url_hash in crawled_hashes:
            print(f"⏭️  Đã crawl, bỏ qua: {url}")
            continue
        else:
            print(f"✅ URL mới phát hiện: {url}")
            new_urls.append(url)
            hashes_to_add.append(url_hash)
            # Thêm vào set tạm thời để tránh trùng lặp ngay trong chính url_list đầu vào
            crawled_hashes.add(url_hash)

    # 3. Ghi tất cả hash mới vào file (chỉ ghi 1 lần)
    if hashes_to_add:
        with open(storage_file, 'a', encoding='utf-8') as f:
            f.write('\n'.join(hashes_to_add) + '\n')
        print(f"📝 Đã lưu {len(hashes_to_add)} mã hash mới vào {storage_file}")

    return new_urls

# history_file = r'tmp/processed_hashes.txt'
# seen = set(open(history_file).read().splitlines()) if os.path.exists(history_file) else set()

def create_task_payload(url, site, correlation_id, task_type, page_num, current_try=1, status_code = [], error_message_list = []):
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

def publish_item(channel, queue, item, persistent = True):
    
    body = json.dumps(item).encode('utf-8')
    props = pika.BasicProperties(delivery_mode=2) if persistent else None
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=body,
        properties=props,
        mandatory=True
    )

def publish_urls(crawler_tool, channel, web_name, sleep_between_pages=1):
    curr_page = 1
    try:
        tmp_page = open(f'tmp/tmp_{web_name}.txt', 'r')
        curr_page = int(tmp_page.read())
    except:
        curr_page = 1

    queue_url = web_name + "_url"
    queue_dlq = web_name + "_dlq"

    channel.queue_declare(queue=queue_dlq)
    channel.queue_declare(queue=queue_url)
    page_count, _ = crawler_tool.getNumberofPage()
  
    session_correlation_id = str(uuid.uuid4())
    log = ContextAdapter(base_logger, {
        "correlation_id": session_correlation_id,
        "component": "Producer"
    })
    log.info(f"Total pages detected: {page_count}")
    count_dup = 0
    for page in range (curr_page, page_count + 1):
        print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        retries = 1
        status_code = None
        error_list = []
        status_code_list = []
        
        url_list, status_code, soup, url_request, origin, error_msg = crawler_tool.getURL(page)
        url_list, status_code, str(soup), url_request, web_name, str(error_msg)
        if not status_code:
            status_code_list.append("")
            error_list.append(error_msg)
        else:
            status_code_list.append(status_code)
            error_list.append(error_msg)
        time.sleep(sleep_between_pages)

        while status_code != 200 and retries < 5:
            log.warning(f"Retry {retries}/5 fetching page", extra={"last_error": error_list[-1] if error_list else None})
            retries+=1
            url_list, status_code, soup, url_request, web_name, error_msg = crawler_tool.getURL(page)
            if not status_code:
                status_code_list.append("")
                error_list.append(error_msg)
            else:
                status_code_list.append(status_code)
                error_list.append(error_msg)
            time.sleep(sleep_between_pages)
        if status_code == 200:
            if len(url_list) != 0:
                new_hashes = []
                # log.info(f"Page {page} success. Found {len(url_list)} items.")
                print(f"Số lượng URL trước khi lọc của trang {page}:" , len(url_list))
                url_list = check_and_add_url(url_list, site=web_name)
                
                if len(url_list) == 0:
                    count_dup += 1
                else:
                    count_dup = 0

                if count_dup > 50:
                    break

                print(f"Số lượng URL sau khi lọc của trang {page}:" , len(url_list))
                for url_item in url_list:
                    
                    payload = create_task_payload(
                        url=url_item, 
                        site=web_name,
                        task_type='CRAWL_DATA', 
                        correlation_id=session_correlation_id,
                        page_num=page
                    )
                    publish_item(channel, queue_url, payload)
                log.info(f"Published {len(url_list)} tasks to {queue_url}")
            else:
                log.error("Page 200 OK but URL list is empty -> Sending to DLQ")
                payload = create_task_payload(
                    url=url_request, 
                    site=web_name, 
                    task_type='CRAWL_LINK', 
                    correlation_id=session_correlation_id,
                    page_num=page,
                    status_code = status_code_list,
                    error_message_list=error_list
                )
                publish_item(channel, queue_dlq, payload)
        else:
            log.error(f"Failed to crawl page after {retries} retries -> Sending to DLQ", extra={"final_status": status_code})
            payload = create_task_payload(
                    url=url_request, 
                    site=web_name,
                    task_type='CRAWL_LINK', 
                    correlation_id=session_correlation_id,
                    page_num=page,
                    status_code = status_code_list,
                    error_message_list=error_list
                )
            publish_item(channel, queue_dlq, payload)
    
        with open(f'tmp/tmp_{web_name}.txt', 'w') as f:
            f.write(str(curr_page))
        time.sleep(sleep_between_pages)

    if os.path.exists(f'tmp/crawled_hashes_{web_name}.txt'):
        os.remove(f'tmp/crawled_hashes_{web_name}.txt')

    if os.path.exists(f'tmp/tmp_{web_name}.txt'):
        os.remove(f'tmp/tmp_{web_name}.txt')
    


