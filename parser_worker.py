import os
import csv
import logging
import time
from datetime import datetime
import redis
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

DATA_DIR = config['data_dir']
OUTPUT_CSV = config['parsed_csv']

REDIS_HOST = config['redis_host']
REDIS_PORT = config['redis_port']
REDIS_DB = config['redis_db']


def parse_header_line(header_line):
    fields = []
    in_field = False
    for i, ch in enumerate(header_line):
        if ch != ' ' and not in_field:
            start = i
            in_field = True
        elif ch == ' ' and in_field:
            end = i
            field_name = header_line[start:end].strip()
            fields.append((field_name, start, end))
            in_field = False
    if in_field:
        field_name = header_line[start:].strip()
        fields.append((field_name, start, len(header_line)))
    return fields


def safe_float(val):
    try:
        return float(val)
    except Exception:
        return None


def f_to_c(f):
    if f is None:
        return ''
    return round((f - 32) * 5.0 / 9.0, 2)


def inch_to_mm(inch):
    if inch is None:
        return ''
    return round(inch * 25.4, 2)


def mph_to_mps(mph):
    if mph is None:
        return ''
    return round(mph * 0.44704, 2)


def mile_to_km(mile):
    if mile is None:
        return ''
    return round(mile * 1.60934, 2)


def convert_units(record):
    # Температуры (F -> C)
    temp_fields = ['TEMP', 'DEWP', 'MAX', 'MIN']
    for f in temp_fields:
        val = safe_float(record.get(f))
        record[f] = f_to_c(val)

    # Видимость (мили -> км)
    visib_val = safe_float(record.get('VISIB'))
    record['VISIB'] = mile_to_km(visib_val)

    # Скорость ветра (миль/ч -> м/с)
    wind_fields = ['WDSP', 'MXSPD', 'GUST']
    for f in wind_fields:
        val = safe_float(record.get(f))
        record[f] = mph_to_mps(val)

    # Осадки (дюймы -> мм)
    prcp_val = safe_float(record.get('PRCP'))
    record['PRCP'] = inch_to_mm(prcp_val)

    # Снег (дюймы -> мм)
    sndp_val = safe_float(record.get('SNDP'))
    record['SNDP'] = inch_to_mm(sndp_val)

    return record


def parse_line_by_fields(line, fields):
    record = {}
    for name, start, end in fields:
        raw_val = line[start:end]
        val = raw_val.rstrip('\n\r')
        val_clean = val.strip().replace('*', '').replace('I', '')
        try:
            val_clean = ' '.join(val_clean.split())
            first_num = val_clean.split(' ')[0]
            val_float = float(first_num)
            record[name] = val_float
        except Exception:
            record[name] = val.strip()

    record = convert_units(record)
    return record


def safe_parse_date(date_value):
    try:
        if isinstance(date_value, float):
            date_str = str(int(date_value))
        else:
            date_str = str(date_value).split('.')[0]
        dt = datetime.strptime(date_str, '%Y%m%d')
        return dt.isoformat()
    except Exception as e:
        logging.warning(f'Ошибка преобразования даты "{date_value}": {e}')
        return None


def convert_date_field(record):
    date_field = None
    for key in record.keys():
        if 'DATE' in key.upper() or 'YEARMODA' in key.upper():
            date_field = key
            break
    if date_field and record.get(date_field):
        dt_iso = safe_parse_date(record[date_field])
        if dt_iso:
            record[date_field] = dt_iso
    return record


def write_records_to_csv(records, fieldnames):
    if not records:
        logging.info('Нет записей для записи')
        return
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        if not file_exists:
            logging.info('Создаём CSV и записываем заголовок')
            writer.writeheader()
        writer.writerows(records)
    logging.info(f'Записано {len(records)} записей в {OUTPUT_CSV}')


def parse_op_file(file_path):
    records = []
    try:
        with open(file_path, encoding='utf-8', errors='ignore') as f:
            header_line = f.readline()
            fields = parse_header_line(header_line)
            fieldnames = [name for name, _, _ in fields]

            if 'flag' not in [fn.lower() for fn in fieldnames]:
                fieldnames.append('flag')


            for line_num, line in enumerate(f, start=2):
                if len(line) < fields[-1][2]:
                    logging.warning(f'Строка {line_num} слишком короткая, пропускаем')
                    continue
                record = parse_line_by_fields(line, fields)
                record = convert_date_field(record)

                if 'flag' not in record and 'flag' in fieldnames:
                    record['flag'] = ''

                records.append(record)

    except Exception as e:
        logging.error(f'Ошибка чтения файла {file_path}: {e}')
        return [], []

    return records, fieldnames


def main_loop():
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

    logging.info('Парсер запущен, ожидание файлов в очереди Redis...')
    while True:
        item = redis_client.blpop('file_queue', timeout=5)
        if item:
            file_path = item[1].decode()
            logging.info(f'Обрабатываем файл {file_path}')
            records, fieldnames = parse_op_file(file_path)
            write_records_to_csv(records, fieldnames)
            try:
                os.remove(file_path)
                logging.info(f'Удалён файл {file_path}')
            except Exception as e:
                logging.error(f'Ошибка удаления файла {file_path}: {e}')
        else:
            logging.debug('Очередь пуста, ждём...')
            time.sleep(config['parser_sleep_sec'])


if __name__ == '__main__':
    main_loop()
