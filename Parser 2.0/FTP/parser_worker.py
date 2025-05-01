import logging
import os
import sys
import time
import json
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.utils import parse_op_file, write_records_to_csv
from modules.redis_helper import get_redis_client, blpop_file
from modules.postgres_helper import get_postgres_connection, write_to_postgres
from modules.logger_setup import setup_logger
from modules.postgres_helper import init_tables

logger = setup_logger('parser')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

OUTPUT_CSV = config['parsed_csv']


def main_loop():
    redis_client = get_redis_client(config)
    pg_conn = get_postgres_connection(config)

    logging.info('Парсер запущен, ожидание файлов в очереди Redis...')
    while True:
        file_path = blpop_file(redis_client, 'file_queue', timeout=5)
        if file_path:
            logger.info(f'Обрабатываем файл {file_path}')

            raw_records, fieldnames = parse_op_file(file_path)

            csv_fieldnames = fieldnames.copy()
            if 'station_id' not in csv_fieldnames:
                csv_fieldnames.append('station_id')

            for record in raw_records:
                record['station_id'] = f"{record.get('STN---', '')}-{record.get('WBAN', '')}"

            write_records_to_csv(raw_records, csv_fieldnames, OUTPUT_CSV)

            pg_records = []
            for record in raw_records:
                try:
                    date_value = record.get('YEARMODA')
                    dt_obj = datetime.datetime.fromisoformat(date_value) if date_value else None

                    pg_records.append((
                        record.get('STN---'),
                        record.get('WBAN'),
                        dt_obj,
                        record.get('TEMP'),
                        record.get('DEWP'),
                        record.get('SLP'),
                        record.get('STP'),
                        record.get('VISIB'),
                        record.get('WDSP'),
                        record.get('MXSPD'),
                        record.get('GUST'),
                        record.get('MAX'),
                        record.get('MIN'),
                        record.get('PRCP'),
                        record.get('SNDP'),
                        record.get('FRSHTT')
                    ))
                except Exception as e:
                    logger.error(f'Ошибка обработки записи: {e}')

            # Запись в PostgreSQL
            write_to_postgres(
                pg_conn,
                config['postgres']['tables']['ftp'],
                pg_records,
                [
                    'stn', 'wban', 'yearmoda', 'temp', 'dewp', 'slp', 'stp',
                    'visib', 'wdsp', 'mxspd', 'gust', 'max_temp', 'min_temp',
                    'prcp', 'sndp', 'frshtt'
                ]
            )

            try:
                os.remove(file_path)
                logger.info(f'Удалён файл {file_path}')
            except Exception as e:
                logger.error(f'Ошибка удаления файла {file_path}: {e}')
        else:
            time.sleep(config['parser_sleep_sec'])


if __name__ == '__main__':
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    pg_conn = get_postgres_connection(config)
    init_tables(pg_conn, config)
    pg_conn.close()
    main_loop()
