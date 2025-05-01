import csv
import os
import logging
from datetime import datetime


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
        return None
    return round((f - 32) * 5.0 / 9.0, 2)


def inch_to_mm(inch):
    if inch is None:
        return None
    return round(inch * 25.4, 2)


def mph_to_mps(mph):
    if mph is None:
        return None
    return round(mph * 0.44704, 2)


def mile_to_km(mile):
    if mile is None:
        return None
    return round(mile * 1.60934, 2)


def convert_units(record):
    temp_fields = ['TEMP', 'DEWP', 'MAX', 'MIN']
    for f in temp_fields:
        val = safe_float(record.get(f))
        if val is not None:
            record[f] = f_to_c(val)

    visib_val = safe_float(record.get('VISIB'))
    if visib_val is not None:
        record['VISIB'] = mile_to_km(visib_val)

    wind_fields = ['WDSP', 'MXSPD', 'GUST']
    for f in wind_fields:
        val = safe_float(record.get(f))
        if val is not None:
            record[f] = mph_to_mps(val)

    prcp_val = safe_float(record.get('PRCP'))
    if prcp_val is not None:
        record['PRCP'] = inch_to_mm(prcp_val)

    sndp_val = safe_float(record.get('SNDP'))
    if sndp_val is not None:
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
    return convert_units(record)


def safe_parse_date(date_value):
    try:
        if isinstance(date_value, str):
            clean_date = date_value.split('T')[0].strip()

            for fmt in ('%Y-%m-%d', '%Y%m%d'):
                try:
                    return datetime.strptime(clean_date, fmt)
                except ValueError:
                    continue
            return None

        elif isinstance(date_value, (int, float)):
            return datetime.strptime(str(int(date_value)), '%Y%m%d')

        return None
    except Exception as e:
        logging.warning(f'Ошибка преобразования даты "{date_value}": {e}')
        return None


def convert_date_field(record):
    date_field = None
    for key in record.keys():
        if 'YEARMODA' in key.upper():
            date_field = key
            break

    if date_field and record.get(date_field):
        parsed_date = safe_parse_date(record[date_field])
        if parsed_date:
            record[date_field] = parsed_date.isoformat()
    return record


def write_records_to_csv(records, fieldnames, output_csv):
    if not records:
        logging.info('Нет записей для записи')
        return

    file_exists = os.path.exists(output_csv) and os.path.getsize(output_csv) > 0

    with open(output_csv, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    logging.info(f'Записано {len(records)} записей в {output_csv}')


def parse_op_file(file_path):
    records = []
    try:
        with open(file_path, encoding='utf-8', errors='ignore') as f:
            header_line = f.readline()
            fields = parse_header_line(header_line)
            fieldnames = [name for name, _, _ in fields]

            for line_num, line in enumerate(f, start=2):
                if len(line) < fields[-1][2]:
                    logging.warning(f'Строка {line_num} слишком короткая, пропускаем')
                    continue

                record = parse_line_by_fields(line, fields)
                record = convert_date_field(record)
                records.append(record)

    except Exception as e:
        logging.error(f'Ошибка чтения файла {file_path}: {e}')
        return [], []
    return records, fieldnames
