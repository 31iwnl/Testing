import logging
import os
import sys
import requests
import time
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.redis_helper import get_redis_client
from modules.logger_setup import setup_logger

logger = setup_logger('downloader')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
URL = "http://ipg.geospace.ru/services/current-space-weather.json"
REDIS_LAST_MODIFIED_KEY = 'space_weather:last_modified'
REDIS_ETAG_KEY = 'space_weather:etag'

def download_and_queue(config):
    redis_client = get_redis_client(config)

    while True:
        try:
            headers = {}
            last_modified = redis_client.get(REDIS_LAST_MODIFIED_KEY)
            etag = redis_client.get(REDIS_ETAG_KEY)
            if last_modified:
                headers['If-Modified-Since'] = last_modified.decode()
            if etag:
                headers['If-None-Match'] = etag.decode()

            response = requests.get(URL, headers=headers, timeout=10)
            if response.status_code == 304:
                logging.info("Данные не изменились, пропускаем скачивание")
            elif response.status_code == 200:
                data = response.json()
                filename = f"current-space-weather_{int(time.time())}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                redis_client.rpush('file_queue', filename)
                logging.info(f"Скачан и добавлен в очередь: {filename}")

                if 'Last-Modified' in response.headers:
                    redis_client.set(REDIS_LAST_MODIFIED_KEY, response.headers['Last-Modified'])
                    logging.info(f"Обновлено Last-Modified: {response.headers['Last-Modified']}")
                if 'ETag' in response.headers:
                    redis_client.set(REDIS_ETAG_KEY, response.headers['ETag'])
                    logging.info(f"Обновлено ETag: {response.headers['ETag']}")
            else:
                logger.warning(f"Неожиданный статус ответа: {response.status_code}")

        except Exception as e:
            logger.error(f"Ошибка при скачивании или сохранении: {e}")

        time.sleep(config.get("downloader_sleep_sec", 60))

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    download_and_queue(config)
