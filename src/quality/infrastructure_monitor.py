import docker
import time
import csv
import os
import matplotlib.pyplot as plt
import requests
import pandas as pd
from datetime import datetime
from multiprocessing import Process
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
INTERVAL = 5 
HISTORY_POINTS = 30 
LOG_DIR = "infrastructure_log"

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

client = docker.from_env()

def get_log_filename():
    return f"{LOG_DIR}/docker_stats_{datetime.now().strftime('%Y-%m-%d')}.csv"

def calculate_cpu_percent(s):
    """Tính % CPU an toàn, tránh lỗi KeyError"""
    try:
        cpu_stats = s.get('cpu_stats', {})
        precpu_stats = s.get('precpu_stats', {})
        
        cpu_delta = cpu_stats.get('cpu_usage', {}).get('total_usage', 0) - \
                    precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
        
        system_delta = cpu_stats.get('system_cpu_usage', 0) - \
                       precpu_stats.get('system_cpu_usage', 0)
        
        if system_delta > 0.0 and cpu_delta > 0.0:
            # Tính toán dựa trên số lượng CPU core hiện có
            cpu_count = len(cpu_stats.get('cpu_usage', {}).get('percpu_usage', [1]))
            return (cpu_delta / system_delta) * cpu_count * 100.0
    except Exception:
        pass
    return 0.0

def get_io_stats(s):
    """Trích xuất I/O an toàn, tránh lỗi NoneType"""
    net_in, net_out, block_in, block_out = 0.0, 0.0, 0.0, 0.0
    
    # Xử lý Network
    networks = s.get('networks')
    if networks:
        for interface in networks.values():
            net_in += interface.get('rx_bytes', 0)
            net_out += interface.get('tx_bytes', 0)
    
    # Xử lý Block I/O
    blkio_stats = s.get('blkio_stats')
    if blkio_stats:
        io_service_bytes = blkio_stats.get('io_service_bytes_recursive')
        if io_service_bytes: # Kiểm tra nếu không phải None
            for stat in io_service_bytes:
                if stat.get('op') == 'Read':
                    block_in += stat.get('value', 0)
                elif stat.get('op') == 'Write':
                    block_out += stat.get('value', 0)

    return net_in/1024, net_out/1024, block_in/(1024*1024), block_out/(1024*1024)

def log_process():
    print("🚀 Tiến trình ghi Log đang vận hành...")
    header = [
        "Timestamp", "Container", "CPU_Perc", 
        "Mem_Usage_MB", "Mem_Limit_MB", "Mem_Perc", 
        "Net_In_KB", "Net_Out_KB", "Block_In_MB", "Block_Out_MB", "PIDs"
    ]
    
    while True:
        filename = get_log_filename()
        file_exists = os.path.isfile(filename)
        
        with open(filename, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(header)
            
            # Lấy danh sách container hiện đang chạy
            containers = client.containers.list()
            for container in containers:
                try:
                    # Lấy stats snapshot
                    s = container.stats(stream=False)
                    
                    # Trích xuất an toàn từng thành phần
                    cpu_perc = calculate_cpu_percent(s)
                    net_in, net_out, block_in, block_out = get_io_stats(s)
                    
                    mem_stats = s.get('memory_stats', {})
                    mem_usage = mem_stats.get('usage', 0) / (1024 * 1024)
                    mem_limit = mem_stats.get('limit', 0) / (1024 * 1024)
                    mem_perc = (mem_usage / mem_limit * 100) if mem_limit > 0 else 0
                    
                    pids = s.get('pids_stats', {}).get('current', 0)

                    writer.writerow([
                        datetime.now().strftime('%H:%M:%S'),
                        container.name,
                        f"{cpu_perc:.2f}",
                        f"{mem_usage:.2f}",
                        f"{mem_limit:.2f}",
                        f"{mem_perc:.2f}",
                        f"{net_in:.2f}",
                        f"{net_out:.2f}",
                        f"{block_in:.2f}",
                        f"{block_out:.2f}",
                        pids
                    ])
                except Exception as e:
                    # Ghi lỗi riêng cho container đó nhưng không làm dừng cả vòng lặp
                    print(f"⚠️ Bỏ qua container {container.name} do lỗi tạm thời: {e}")
            f.flush()
        time.sleep(INTERVAL)

def telegram_process():
    print("🚀 Tiến trình Telegram đang vận hành...")
    last_msg_id = None
    
    while True:
        time.sleep(INTERVAL)
        filename = get_log_filename()
        if not os.path.exists(filename): continue
        
        try:
            df = pd.read_csv(filename)
            if df.empty or len(df['Container'].unique()) == 0: continue
            
            # Chỉ lấy các mốc thời gian gần nhất cho biểu đồ
            unique_containers = df['Container'].unique()
            plt.figure(figsize=(10, 6))
            
            for name in unique_containers:
                subset = df[df['Container'] == name].tail(HISTORY_POINTS)
                plt.plot(subset['Timestamp'], subset['CPU_Perc'].astype(float), label=name)
            
            plt.title(f"CPU Monitoring - {datetime.now().strftime('%H:%M:%S')}")
            plt.ylabel("CPU %")
            plt.legend(loc='upper left', fontsize='x-small', ncol=2)
            plt.xticks(rotation=45)
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.tight_layout()
            
            img_path = "live_stats.png"
            plt.savefig(img_path)
            plt.close()

            if last_msg_id:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/deleteMessage", 
                              data={"chat_id": CHAT_ID, "message_id": last_msg_id})
            
            with open(img_path, "rb") as photo:
                r = requests.post(f"https://api.telegram.org/bot{TOKEN}/sendPhoto", 
                                  data={"chat_id": CHAT_ID, "caption": "📊 Biểu đồ CPU thực tế"}, 
                                  files={"photo": photo})
                if r.status_code == 200:
                    last_msg_id = r.json()['result']['message_id']
        except Exception as e:
            print(f"⚠️ Lỗi Telegram: {e}")

if __name__ == '__main__':
    p1 = Process(target=log_process)
    p1.start()
    p1.join()