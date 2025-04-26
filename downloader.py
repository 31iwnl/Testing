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

ASUSTEM_CSV = config['stations_csv']

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
        # self.allowed_stations = self.load_allowed_stations()

    # def load_allowed_stations(self):
    #     """Loads authorized stations from the CSV file"""
    #     try:
    #         with open(ASUSTEM_CSV, 'r', encoding='utf-8') as f:
    #             reader = csv.DictReader(f)
    #             station_set = set(f"{row['USAF']}-{row['WBAN']}" for row in reader if row['FLAG'] == 'RUS')
    #         logging.info(f"Loaded {len(station_set)} authorized stations from {ASUSTEM_CSV}")
    #         return station_set
    #     except Exception as e:
    #         logging.error(f"Error loading {ASUSTEM_CSV}: {e}")
    #         return set()

    def connect(self):
        for attempt in range(self.max_retries):
            try:
                self.ftp = ftplib.FTP(self.host, timeout=30)
                self.ftp.login(self.user, self.password)
                self.ftp.cwd(FTP_BASE_DIR)
                logging.info(f'Подключено к FTP {self.host}')
                return
            except Exception as e:
                logging.warning(f'Попытка подключения к FTP {attempt + 1} не удалась: {e}')
                time.sleep(self.retry_delay)
        raise ConnectionError(f'Подключиться к FTP не удалось {self.host} после {self.max_retries} попыток')

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
                logging.info('Отключено')
            except Exception as e:
                logging.warning(f'Ошибка подключения к FTP: {e}')
            self.ftp = None

    def safe_ftp_command(self, cmd):
        for attempt in range(self.max_retries):
            try:
                return self.ftp.sendcmd(cmd)
            except (
                ftplib.error_temp, ftplib.error_reply, ftplib.error_proto, ftplib.error_perm,
                ConnectionResetError) as e:
                logging.warning(f'FTP команда "{cmd}" не сработала на попытке {attempt + 1}/{self.max_retries}: {e}')
            self.reconnect()
            time.sleep(self.retry_delay)
        raise Exception(f'FTP команда "{cmd}" не сработала после {self.max_retries} попыток')

    def reconnect(self):
        logging.info('Переподключаюсь к FTP серверу...')
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
            logging.warning(f'Не получен MDTM для {filename}: {e}')
        return None

    def get_redis_mdtm(self, key):
        val = self.redis_client.hget(self.redis_key_prefix, key)
        if val:
            return val.decode()
        return None

    def set_redis_mdtm(self, key, mdtm_iso):
        self.redis_client.hset(self.redis_key_prefix, key, mdtm_iso)

    def download_file(self, remote_file, local_file):
        for attempt in range(self.max_retries):
            try:
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                with open(local_file, 'wb') as f:
                    self.ftp.retrbinary(f'RETR {remote_file}', f.write)
                logging.info(f'Скачен {remote_file}')
                return True
            except (
                    ftplib.error_temp, ftplib.error_reply, ftplib.error_proto, ftplib.error_perm,
                    ConnectionResetError) as e:
                logging.warning(f'Ошибка скачивания {remote_file} попытка {attempt + 1}/{self.max_retries}: {e}')
                self.reconnect()
                time.sleep(self.retry_delay)
        logging.error(f'Не получилось скачать {remote_file} после {self.max_retries} попыток')
        return False

    def download_and_unpack(self, year, filename):
        # base_name = os.path.splitext(filename)[0]
        # if base_name not in self.allowed_stations:
        #     logging.info(f'Station {base_name} is not in the list of authorized stations, skipping file {filename}')
        #     return

        local_gz = os.path.join(self.local_dir, filename)
        local_op = local_gz[:-3]
        ftp_path = f'{FTP_BASE_DIR}/{year}/{filename}'

        mdtm = self.get_ftp_file_mdtm(filename)
        mdtm_iso = mdtm.isoformat() if mdtm else None

        redis_mdtm = self.get_redis_mdtm(ftp_path)
        if redis_mdtm == mdtm_iso:
            logging.info(f'Файл {filename} существует, пропускаем')
            return

        if not self.download_file(filename, local_gz):
            logging.error(f'Пропустить не получилось, ошибка: {filename}')
            return

        try:
            with gzip.open(local_gz, 'rb') as f_in, open(local_op, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(local_gz)
            logging.info(f'Скачен {filename}')
        except Exception as e:
            logging.error(f'Ошибка при скачивании {filename}: {e}')
            return

        self.set_redis_mdtm(ftp_path, mdtm_iso)

        # Limit queue size
        queue_len = self.redis_client.llen('file_queue')
        queue_limit = 3  # Setting Queue Limit Here

        if queue_len < queue_limit:
            self.redis_client.rpush('file_queue', local_op)
            logging.info(f'Файл добавлен: {local_op}')
        else:
            logging.warning(f'Размер превышен, пропуск {local_op}')

        queue_len = self.redis_client.llen('file_queue')
        logging.info(f'Текущая очереь Редис: {queue_len}')

    def download_year_files(self, year):
        try:
            self.ftp.cwd(year)
        except Exception as e:
            logging.warning(f'Папка {year} не найдена: {e}')
            return

        try:
            files = self.ftp.nlst()
        except Exception as e:
            logging.warning(f'Список файлов {year} не доступен: {e}')
            return

        op_files = [f for f in files if f.endswith('.op.gz')]

        for i, fname in enumerate(op_files, 1):
            self.download_and_unpack(year, fname)
            time.sleep(config['download_pause_sec'])
            logging.info(f'Скачено {i}/{len(op_files)} файлов за год {year}')

        self.ftp.cwd('..')

    def download_all(self):
        self.connect()
        try:
            years = [d for d in self.ftp.nlst() if d.isdigit()]
        except Exception as e:
            logging.error(f'Ошибка при подсчете файлов: {e}')
            self.disconnect()
            return

        if self.start_year:
            years = [y for y in years if int(y) >= int(self.start_year)]
        total_years = len(years)

        for i, year in enumerate(sorted(years), 1):
            logging.info(f'Скачивается {year} ({i}/{total_years})')
            self.download_year_files(year)

        self.disconnect()
        logging.info('Скачено')

def main():
    start_year = None
    if len(sys.argv) > 1:
        try:
            start_year = int(sys.argv[1])
        except ValueError:
            logging.error('start_year должно быть числом')
            sys.exit(1)

    agent = FTPAgent(start_year=start_year)
    try:
        agent.download_all()
    except Exception as e:
        logging.error(f'Ошибка: {e}')

if __name__ == '__main__':
    main()
