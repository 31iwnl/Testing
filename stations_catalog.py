import ftplib
import csv
import json
import logging
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

ISD_HISTORY_FTP = 'ftp.ncdc.noaa.gov'
ISD_HISTORY_PATH = '/pub/data/noaa/isd-history.csv'
ASUSTEM_STATIONS_CSV = config['stations_csv']


# Approximate boundaries of Russia
# RUSSIA_BOUNDS = {
#     'lat_min': 41.0,
#     'lat_max': 82.0,
#     'lon_min': 19.0,
#     'lon_max': 180.0
# }

def download_isd_history_csv():
    ftp = ftplib.FTP(ISD_HISTORY_FTP)
    ftp.login()
    r = io.BytesIO()
    ftp.retrbinary(f'RETR {ISD_HISTORY_PATH}', r.write)
    ftp.quit()
    return r.getvalue().decode('utf-8', errors='replace')


def filter_russian_stations(csv_text):
    reader = csv.DictReader(csv_text.splitlines())
    stations = list(reader)
    return stations


def save_stations_to_csv(stations, output_file):
    if not stations:
        logging.warning("No stations to save.")
        return

    fieldnames = stations[0].keys()  # Get all fieldnames from the first station
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stations)
    logging.info(f"Saved {len(stations)} stations to {output_file}")


def main():
    csv_text = download_isd_history_csv()
    stations = filter_russian_stations(csv_text)
    save_stations_to_csv(stations, ASUSTEM_STATIONS_CSV)


if __name__ == '__main__':
    main()
