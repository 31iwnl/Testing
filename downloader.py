import ftplib
import json
import os
import gzip
import shutil
import logging
import time
from datetime import datetime
import redis
import csv
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

FTP_HOST = config['ftp_host']
FTP_BASE_DIR = config['ftp_base_dir']
DATA_DIR = config['data_dir']

REDIS_HOST = config['redis_host']
REDIS_PORT = config['redis_port']
REDIS_DB = config['redis_db']

OUTPUT_CSV = config.get('output_csv', 'output.csv')


class FTPAgent:
    def __init__(self, start_year=None, max_retries=3, retry_delay=5):
        self.host = FTP_HOST
        self.user = 'anonymous'
        self.password = ''
        self.local_dir = os.path.abspath(DATA_DIR)
        os.makedirs(self.local_dir, exist_ok=True)
        self.start_year = start_year
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ftp = None
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.redis_key_prefix = 'downloaded_files'
        self.allowed_stations = self.load_allowed_stations()

    def load_allowed_stations(self):
        allowed_stations = set()
        try:
            if os.path.exists(OUTPUT_CSV):
                with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('flag', '').lower() == 'true':
                            usaf = row.get('USAF', '').strip()
                            wban = row.get('WBAN', '').strip()
                            if usaf and wban:
                                allowed_stations.add(f"{usaf}-{wban}")
                logging.info(f"Загружено {len(allowed_stations)} разрешенных станций из {OUTPUT_CSV} с flag=true")
            else:
                logging.warning(f"CSV файл {OUTPUT_CSV} не найден. Фильтрация станций не будет производиться.")
        except Exception as e:
            logging.error(f"Ошибка при загрузке разрешенных станций из {OUTPUT_CSV}: {e}")
        return allowed_stations

    def connect(self):
        for attempt in range(self.max_retries):
            try:
                self.ftp = ftplib.FTP(self.host, timeout=30)
                self.ftp.login(self.user, self.password)
                logging.info(f'Подключено к FTP {self.host}')
                self.ftp.cwd(FTP_BASE_DIR)
                logging.info(f'Переход в директорию {FTP_BASE_DIR}')
                return
            except Exception as e:
                logging.warning(f'Попытка подключения к FTP {attempt + 1} не удалась: {e}')
                time.sleep(self.retry_delay)
        raise ConnectionError(f'Не удалось подключиться к FTP {self.host} после {self.max_retries} попыток')

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
                logging.info('Отключено от FTP-сервера')
            except Exception as e:
                logging.warning(f'Ошибка при отключении от FTP: {e}')
            self.ftp = None

    def safe_ftp_command(self, cmd):
        for attempt in range(self.max_retries):
            try:
                return self.ftp.sendcmd(cmd)
            except (
                    ftplib.error_temp, ftplib.error_reply, ftplib.error_proto, ftplib.error_perm,
                    ConnectionResetError) as e:
                logging.warning(f'FTP команда "{cmd}" не удалась при попытке {attempt + 1}/{self.max_retries}): {e}')
            self.reconnect()
            time.sleep(self.retry_delay)
        raise Exception(f'FTP команда "{cmd}" не удалась после {self.max_retries} попыток')

    def reconnect(self):
        logging.info('Переподключение к FTP-серверу...')
        self.disconnect()
        self.connect()

    def get_ftp_file_mdtm(self, filename):
        try:
            resp = self.safe_ftp_command(f'MDTM {filename}')
            if resp.startswith('213 '):
                dt_str = resp[4:].strip()
                dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
                return dt
        except Exception as e:
            logging.warning(f'Не удалось получить MDTM для {filename}: {e}')
        return None

    def get_redis_mdtm(self, key):
        val = self.redis_client.hget(self.redis_key_prefix, key)
        if val:
            return val.decode()
        return None

    def set_redis_mdtm(self, key, mdtm_iso):
        self.redis_client.hset(self.redis_key_prefix, key, mdtm_iso)

    def download_file(self, remote_file, local_file):
        try:
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
        except Exception as e:
            logging.warning(f'Не удалось создать директорию: {e}')
        for attempt in range(self.max_retries):
            try:
                with open(local_file, 'wb') as f:
                    self.ftp.retrbinary(f'RETR {remote_file}', f.write)
                return True
            except (
                    ftplib.error_temp, ftplib.error_reply, ftplib.error_proto, ftplib.error_perm,
                    ConnectionResetError) as e:
                logging.warning(f'Ошибка при скачивании {remote_file} (попытка {attempt + 1}/{self.max_retries}): {e}')
                self.reconnect()
                time.sleep(self.retry_delay)
        logging.error(f'Не удалось скачать {remote_file} после {self.max_retries} попыток')
        return False

    def download_and_unpack(self, year, filename):
        ftp_path = f'{FTP_BASE_DIR}/{year}/{filename}'
        local_gz = os.path.join(self.local_dir, filename)
        local_op = local_gz[:-3]

        mdtm = self.get_ftp_file_mdtm(filename)
        mdtm_iso = mdtm.isoformat() if mdtm else None
        redis_mdtm = self.get_redis_mdtm(ftp_path)
        if redis_mdtm == mdtm_iso:
            logging.info(f'Файл {filename} уже обработан (найден в Redis с совпадающим MDTM), пропуск скачивания')
            return
        # Если файла нет в Redis или MDTM не совпадает - скачиваем
        redis_mdtm = self.get_redis_mdtm(ftp_path)
        if redis_mdtm == mdtm_iso:  # Если MDTM совпадает, то пропускаем скачивание
            logging.info(f'Файл {filename} уже обработан (найден в Redis с совпадающим MDTM), пропуск скачивания')
            return

        if not self.download_file(filename, local_gz):
            logging.error(f'Не удалось скачать {filename}, пропуск')
            return

        try:
            with gzip.open(local_gz, 'rb') as f_in, open(local_op, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(local_gz)
            logging.info(f'Распаковано {filename}')
        except Exception as e:
            logging.error(f'Ошибка при распаковке {filename}: {e}')
            return

        self.set_redis_mdtm(ftp_path, mdtm_iso)
        queue_len = self.redis_client.llen('file_queue')
        queue_limit = 3

        if queue_len < queue_limit:
            self.redis_client.rpush('file_queue', local_op)
            logging.info(f'Файл добавлен в очередь: {local_op}')
        else:
            logging.warning(f'Превышен лимит очереди, пропуск {local_op}')

        queue_len = self.redis_client.llen('file_queue')
        logging.info(f'Текущая длина очереди Redis: {queue_len}')

    def download_year_files(self, year):
        try:
            self.ftp.cwd(year)
        except Exception as e:
            logging.warning(f'Директория {year} не найдена: {e}')
            return

        try:
            files = self.ftp.nlst()
        except Exception as e:
            logging.warning(f'Не удалось получить список файлов в {year}: {e}')
            return

        op_files = [f for f in files if f.endswith('.op.gz')]

        for i, fname in enumerate(op_files, 1):
            parts = fname.split('-')
            if len(parts) >= 2:
                station_key = f"{parts[0]}-{parts[1]}"
            else:
                station_key = None

            if station_key not in self.allowed_stations:
                logging.info(f'Станция {fname} не входит в список разрешенных станций, пропуск файла')
                continue

            # Проверяем, нужно ли скачивать файл
            should_download = False
            # Проверяем, есть ли запись о файле в Redis и совпадает ли MDTM
            ftp_path = f'{FTP_BASE_DIR}/{year}/{fname}'
            redis_mdtm = self.get_redis_mdtm(ftp_path)
            mdtm = self.get_ftp_file_mdtm(fname)
            mdtm_iso = mdtm.isoformat() if mdtm else None
            if redis_mdtm != mdtm_iso:  # Если не совпадает, то нужно качать
                should_download = True

            self.download_and_unpack(year, fname)

            time.sleep(config['download_pause_sec'])

            if should_download:  # Логируем, только если нужно было скачать
                logging.info(f'Скачано {i}/{len(op_files)} файлов за год {year}')

        self.ftp.cwd('..')

    def download_all(self):
        self.connect()
        try:
            years = [d for d in self.ftp.nlst() if d.isdigit()]
        except Exception as e:
            logging.error(f'Ошибка при получении списка директорий: {e}')
            self.disconnect()
            return

        if self.start_year:
            years = [y for y in years if int(y) >= int(self.start_year)]
        total_years = len(years)

        for i, year in enumerate(sorted(years), 1):
            logging.info(f'Скачивание данных за {year} год ({i}/{total_years})')
            self.download_year_files(year)

        self.disconnect()
        logging.info('Скачивание завершено')


def main():
    start_year = None
    if len(sys.argv) > 1:
        try:
            start_year = int(sys.argv[1])
        except ValueError:
            logging.error('start_year должно быть целым числом')
            sys.exit(1)

    agent = FTPAgent(start_year=start_year)
    try:
        agent.download_all()
    except Exception as e:
        logging.error(f'Произошла ошибка: {e}')


if __name__ == '__main__':
    main()
