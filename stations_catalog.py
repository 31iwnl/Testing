import csv
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

INPUT_CSV = config.get('input_csv', 'input.csv')
OUTPUT_CSV = config.get('output_csv', 'output.csv')

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
            logging.error(
                f"Attempt {attempt + 1}/{max_retries} error for {'proxy' if use_proxy else 'direct'} request: {e}")
            time.sleep(2)
    return False, ''


def load_processed_usaf(csv_file):
    processed = set()
    if not os.path.isfile(csv_file):
        return processed
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            usaf = row.get('USAF')
            if usaf:
                processed.add(usaf.strip())
    logging.info(f"Loaded {len(processed)} processed USAF from {csv_file}")
    return processed


def append_station_to_csv(station_row, csv_file, fieldnames, processed_usaf):
    usaf = station_row.get('USAF', '').strip()
    if usaf in processed_usaf:
        return
    with lock:
        file_exists = os.path.isfile(csv_file)
        write_header = not file_exists or os.path.getsize(csv_file) == 0
        with open(csv_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(station_row)
        processed_usaf.add(usaf)


def process_row(row, fieldnames, processed_usaf, use_proxy):
    usaf = row.get('USAF', '').strip()
    if not usaf or usaf in processed_usaf:
        return

    lat_str = row.get('LAT', '').strip()
    lon_str = row.get('LON', '').strip()
    via = 'proxy' if use_proxy else 'direct'

    if not lat_str or not lon_str:
        row_copy = row.copy()
        row_copy['flag'] = ''
        row_copy['display_name'] = ''
        row_copy['request_via'] = via
        append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf)
        return

    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except ValueError:
        row_copy = row.copy()
        row_copy['flag'] = ''
        row_copy['display_name'] = ''
        row_copy['request_via'] = via
        append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf)
        return

    is_russia, display_name = safe_nominatim_reverse_geocode(lat, lon, use_proxy=use_proxy)

    row_copy = row.copy()
    row_copy['flag'] = 'true' if is_russia else 'false'
    row_copy['display_name'] = display_name
    row_copy['request_via'] = via
    append_station_to_csv(row_copy, OUTPUT_CSV, fieldnames, processed_usaf)

    logging.info(f"USAF {usaf} checked via {via}, flag {row_copy['flag']}")


def main():
    if not os.path.isfile(INPUT_CSV):
        raise FileNotFoundError(f"Input CSV file {INPUT_CSV} not found")

    processed_usaf = load_processed_usaf(OUTPUT_CSV)

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
            futures.append(executor.submit(process_row, row, fieldnames, processed_usaf, use_proxy))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing row: {e}")


if __name__ == '__main__':
    main()
