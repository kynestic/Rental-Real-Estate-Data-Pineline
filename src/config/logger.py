# tools/logger.py
import logging
import json
import sys
import requests
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        
        # Lấy thêm context từ extra (task_id, correlation_id)
        for key in ["task_id", "correlation_id"]:
            if hasattr(record, key):
                log_record[key] = getattr(record, key)
        return json.dumps(log_record, ensure_ascii=False)

class TelegramHandler(logging.Handler):
    def __init__(self, token, chat_id):
        super().__init__()
        self.token = token
        self.chat_id = chat_id

    def emit(self, record):
        # Chỉ gửi khi log là ERROR hoặc cao hơn
        if record.levelno >= logging.ERROR:
            try:
                message = self.format(record)
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                # Gửi message dưới dạng code block để dễ đọc JSON trên điện thoại
                payload = {
                    "chat_id": self.chat_id,
                    "text": f"🚨 **Crawler Alert**\n`{message}`",
                    "parse_mode": "Markdown"
                }
                requests.post(url, json=payload, timeout=5)
            except Exception:
                pass # Tránh làm sập app chính nếu Telegram lỗi

def get_logger(name, log_file="system.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Cấu hình Formatter chung
        formatter = JsonFormatter()

        # 1. Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. File Handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 3. Telegram Handler (THAY ID CỦA BẠN VÀO ĐÂY)
        bot_token = "8264995128:AAGA7HIJstU7UgOJmYTywSeLMRYvtptFvAw"
        my_chat_id = "7259510958" 
        
        tele_handler = TelegramHandler(bot_token, my_chat_id)
        tele_handler.setFormatter(formatter)
        logger.addHandler(tele_handler)
        
    return logger

class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get('extra', {})
        extra.update(self.extra)
        kwargs['extra'] = extra
        return msg, kwargs