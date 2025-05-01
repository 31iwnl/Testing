import os
import sys
import json
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.redis_helper import get_redis_client, blpop_file
from modules.utils import write_records_to_csv
from modules.postgres_helper import get_postgres_connection, write_to_postgres
from modules.logger_setup import setup_logger
from modules.postgres_helper import init_tables
logger = setup_logger('parser')


def parse_space_weather_json(filepath):
    try:
        with open(filepath, encoding='utf-8') as f:
            data = json.load(f)
        records = []

        # X-Ray данные
        xray = data.get('xray', {})
        records.append({
            'type': 'xray',
            'time': xray.get('time'),
            'value': xray.get('ball'),
            'description': xray.get('description')
        })

        # Магнитное поле
        magnit = data.get('magnit', {})
        records.append({
            'type': 'magnit',
            'time': magnit.get('time'),
            'value': magnit.get('kp'),
            'description': magnit.get('description')
        })

        # Частицы
        particles = data.get('particles', {})
        records.append({
            'type': 'particles',
            'time': particles.get('time'),
            'value': particles.get('ball'),
            'description': particles.get('description')
        })

        return records
    except Exception as e:
        logger.error(f"Ошибка при парсинге {filepath}: {e}")
        return []


def main_loop(config):
    redis_client = get_redis_client(config)
    pg_conn = get_postgres_connection(config)

    output_csv = config.get('space_weather', 'space_weather.csv')
    fieldnames = ['type', 'time', 'value', 'description']

    logger.info('Парсер запущен, ожидание файлов в очереди Redis...')
    while True:
        file_path = blpop_file(redis_client, 'file_queue', timeout=5)
        if file_path:
            logger.info(f'Обрабатываем файл {file_path}')

            # Парсинг и запись
            records = parse_space_weather_json(file_path)
            write_records_to_csv(records, fieldnames, output_csv)

            # Подготовка данных для PostgreSQL
            pg_records = [
                (r['type'], r['time'], r['value'], r['description'])
                for r in records
            ]
            write_to_postgres(
                pg_conn,
                config['postgres']['tables']['space_weather'],
                pg_records,
                fieldnames
            )

            # Очистка
            try:
                os.remove(file_path)
                logger.info(f'Удалён файл {file_path}')
            except Exception as e:
                logger.error(f'Ошибка удаления файла {file_path}: {e}')
        else:
            time.sleep(config.get('parser_sleep_sec', 10))


if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), '..', 'config.json'), 'r', encoding='utf-8') as f:
        config = json.load(f)

    pg_conn = get_postgres_connection(config)

    init_tables(pg_conn, config)
    pg_conn.close()

    main_loop(config)
