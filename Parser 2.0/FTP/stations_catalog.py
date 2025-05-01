import csv
import ftplib
import io
import json
import logging
import os
import sys
import threading
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle

import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.logger_setup import setup_logger

logger = setup_logger('catalog')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
PROJECT_ROOT = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

# Путь к config.json
config_path = os.path.join(PROJECT_ROOT, 'config.json')
with open(config_path, 'r', encoding='utf-8') as f:
    config = json.load(f)

# Абсолютные пути к CSV-файлам
INPUT_CSV = os.path.join(PROJECT_ROOT, config.get('input_csv', 'input.csv'))
OUTPUT_CSV = os.path.join(PROJECT_ROOT, config.get('output_csv', 'output.csv'))

PROXIES = [
    'http://proxy:g1ANEx@5.187.7.142:3128'
]

EMAILS = config.get('emails', [
    'email1@example.com',
    'email2@example.com',
    'email3@example.com'
])

NOMINATIM_URL = 'https://nominatim.openstreetmap.org/reverse'
proxy_cycle = cycle(PROXIES)
email_cycle = cycle(EMAILS)

lock = threading.Lock()
last_request_time = {}


def get_ftp_file_mdtm(ftp, filepath):
    try:
        resp = ftp.sendcmd(f'MDTM {filepath}')
        if resp.startswith('213 '):
            dt_str = resp[4:].strip()
            dt = datetime.datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            return dt
    except Exception as e:
        logger.warning(f"Failed to get MDTM for {filepath}: {e}")
    return None


def download_isd_history_csv(output_file, max_retries=5):
    """
    Скачивает isd-history.csv с FTP, корректно закрывает соединение даже при ошибках.
    """
    for attempt in range(max_retries):
        ftp = None
        try:
            ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
            ftp.login()
            remote_path = '/pub/data/noaa/isd-history.csv'

            remote_mdtm = get_ftp_file_mdtm(ftp, remote_path)
            if remote_mdtm is None:
                logger.warning("Could not get remote file modification time, downloading anyway")
            else:
                if os.path.isfile(output_file):
                    local_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(output_file))
                    if local_mtime >= remote_mdtm:
                        logging.info("Local file is up to date, skipping download")
                        return True

            r = io.BytesIO()
            ftp.retrbinary(f'RETR {remote_path}', r.write)

            text = r.getvalue().decode('utf-8', errors='replace')
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                f.write(text)

            if remote_mdtm:
                mod_time = remote_mdtm.timestamp()
                os.utime(output_file, (mod_time, mod_time))

            logging.info(f"Downloaded ISD history CSV to {output_file}")
            return True
        except ftplib.error_temp as e:
            if '530' in str(e):
                logger.warning(f"FTP connection limit reached, waiting 60 seconds (attempt {attempt+1}/{max_retries})")
                time.sleep(60)
            else:
                logger.error(f"FTP temporary error: {e}")
                break
        except Exception as e:
            logger.error(f"Failed to download ISD history CSV: {e}")
            break
        finally:
            if ftp:
                try:
                    ftp.quit()
                except Exception:
                    pass
    return False


def safe_nominatim_reverse_geocode(lat, lon, use_proxy=False, max_retries=3):
    for attempt in range(max_retries):
        email = next(email_cycle)
        headers = {'User-Agent': f'MyApp/1.0 ({email})'}
        proxies = None
        if use_proxy:
            proxy = next(proxy_cycle)
            proxies = {'http': proxy, 'https': proxy}

        with lock:
            now = time.time()
            key = proxy if use_proxy else 'direct'
            last_time = last_request_time.get(key, 0)
            elapsed = now - last_time
            if elapsed < 1.1:
                time.sleep(1.1 - elapsed)

        params = {
            'format': 'json',
            'lat': lat,
            'lon': lon,
            'zoom': 18,
            'addressdetails': 1,
            'email': email
        }

        try:
            response = requests.get(NOMINATIM_URL, params=params, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status()
            data = response.json()
            country_code = data.get('address', {}).get('country_code', '').lower()
            display_name = data.get('display_name', '')
            is_russia = (country_code == 'ru')
            with lock:
                last_request_time[key] = time.time()
            return is_russia, display_name
        except Exception as e:
            logger.error(
                f"Attempt {attempt + 1}/{max_retries} error for {'proxy' if use_proxy else 'direct'} request: {e}")
            time.sleep(2)
    return False, ''


def load_processed_usaf_wban(csv_file):
    processed = set()
    if not os.path.isfile(csv_file):
        return processed
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            usaf = row.get('USAF', '').strip()
            wban = row.get('WBAN', '').strip()
            if usaf and wban:
                processed.add((usaf, wban))
    logging.info(f"Loaded {len(processed)} processed USAF+WBAN from {csv_file}")
    return processed


def append_station_to_csv(station_row, csv_file, fieldnames, processed_usaf_wban):
    usaf = station_row.get('USAF', '').strip()
    wban = station_row.get('WBAN', '').strip()
    key = (usaf, wban)
    if key in processed_usaf_wban:
        return
    with lock:
        file_exists = os.path.isfile(csv_file)
        write_header = not file_exists or os.path.getsize(csv_file) == 0
        with open(csv_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(station_row)
        processed_usaf_wban.add(key)


def process_row(row, fieldnames, processed_usaf_wban, use_proxy):
    usaf = row.get('USAF', '').strip()
    wban = row.get('WBAN', '').strip()
    key = (usaf, wban)
    if not usaf or not wban or key in processed_usaf_wban:
        return

    lat_str = row.get('LAT', '').strip()
    lon_str = row.get('LON', '').strip()
    via = 'proxy' if use_proxy else 'direct'

    if not lat_str or not lon_str:
        row_copy = row.copy()
        row_copy['flag'] = ''
        row_copy['display_name'] = ''
        row_copy['request_via'] = via
        append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf_wban)
        return

    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except ValueError:
        row_copy = row.copy()
        row_copy['flag'] = ''
        row_copy['display_name'] = ''
        row_copy['request_via'] = via
        append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf_wban)
        return

    is_russia, display_name = safe_nominatim_reverse_geocode(lat, lon, use_proxy=use_proxy)

    row_copy = row.copy()
    row_copy['flag'] = 'true' if is_russia else 'false'
    row_copy['display_name'] = display_name
    row_copy['request_via'] = via
    append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf_wban)

    logging.info(f"USAF {usaf}, WBAN {wban} checked via {via}, flag {row_copy['flag']}")


def main():
    if not os.path.isfile(INPUT_CSV):
        logging.info(f"{INPUT_CSV} not found, downloading from FTP...")
        if not download_isd_history_csv(INPUT_CSV):
            raise FileNotFoundError(f"Cannot find or download {INPUT_CSV}")

    processed_usaf_wban = load_processed_usaf_wban(OUTPUT_CSV)

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    fieldnames = reader.fieldnames.copy()
    for col in ['flag', 'display_name', 'request_via']:
        if col not in fieldnames:
            fieldnames.append(col)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for idx, row in enumerate(rows):
            use_proxy = (idx % 2 == 1)  # нечётные строки proxy, чётные direct
            futures.append(executor.submit(process_row, row, fieldnames, processed_usaf_wban, use_proxy))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing row: {e}")


if __name__ == '__main__':
    main()
