import subprocess
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def run_process(cmd, name):
    logging.info(f'Запускаем процесс: {name}')
    return subprocess.Popen(cmd)

def main():
    python = sys.executable

    # Сценарии запуска
    modes = {
        'ftp': [
            [python, 'FTP/downloader.py'],
            [python, 'FTP/parser_worker.py'],
            [python, 'FTP/stations_catalog.py']
        ],
        'ecomonitoring': [
            [python, 'ecomonitoring/downloader.py'],
            [python, 'ecomonitoring/parser.py']
        ],
        'all': [
            [python, 'FTP/downloader.py'],
            [python, 'FTP/parser_worker.py'],
            [python, 'FTP/stations_catalog.py'],
            [python, 'ecomonitoring/downloader.py'],
            [python, 'ecomonitoring/parser.py']
        ]
    }

    # Парсим аргумент командной строки
    if len(sys.argv) < 2 or sys.argv[1].lower() not in modes:
        print("Использование: python run_parser.py [ftp|ecomonitoring|all]")
        sys.exit(1)

    mode = sys.argv[1].lower()
    procs = []

    try:
        # Запуск всех процессов выбранной группы
        for cmd in modes[mode]:
            procs.append(run_process(cmd, ' '.join(cmd[1:])))

        # Следим за процессами и перезапускаем при падении
        while True:
            for i, proc in enumerate(procs):
                retcode = proc.poll()
                if retcode is not None:
                    logging.warning(f'Процесс {procs[i].args[1]} завершился с кодом {retcode}, перезапускаем...')
                    procs[i] = run_process(procs[i].args, procs[i].args[1])
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info('Прерывание, завершаем процессы...')
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait()
        logging.info('Все процессы завершены')

if __name__ == '__main__':
    main()
