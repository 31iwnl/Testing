import subprocess
import sys
import logging
import time
import os

logging.basicConfig(level=logging.INFO)


def run_process(cmd, name):
    logging.info(f'Запускаем процесс: {name}')
    return subprocess.Popen(cmd)


def main():
    python = sys.executable

    logging.info('Обновляем stations...')
    subprocess.run([python, 'stations_catalog.py'], check=True)
    logging.info('stations обновлён.')

    downloader = run_process([python, 'downloader.py'], 'downloader.py')
    parser_worker = run_process([python, 'parser_worker.py'], 'parser_worker.py')

    try:
        while True:
            if downloader.poll() is not None:
                logging.info('downloader.py завершил работу')
                break
            if parser_worker.poll() is not None:
                logging.info('parser_worker.py завершил работу')
                break
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info('Прерывание, завершаем процессы...')
        downloader.terminate()
        parser_worker.terminate()

    downloader.wait()
    parser_worker.wait()
    logging.info('Все процессы завершены')


if __name__ == '__main__':
    main()
