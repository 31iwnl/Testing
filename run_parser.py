import subprocess
import sys
import logging
import time

logging.basicConfig(level=logging.INFO)

def run_process(cmd, name):
    logging.info(f'Запускаем процесс: {name}')
    return subprocess.Popen(cmd)

def main():
    python = sys.executable

    procs = {
        'downloader.py': run_process([python, 'downloader.py'], 'downloader.py'),
        'parser_worker.py': run_process([python, 'parser_worker.py'], 'parser_worker.py'),
    }

    stations_cmd = [python, 'stations_catalog.py']
    stations_proc = None
    stations_last_run = 0  # время последнего завершения
    stations_cooldown = 5 * 60  # 5 минут в секундах

    try:
        while True:
            # Перезапускаем downloader и parser_worker при падении
            for name in ['downloader.py', 'parser_worker.py']:
                proc = procs[name]
                retcode = proc.poll()
                if retcode is not None:
                    logging.warning(f'Процесс {name} завершился с кодом {retcode}, перезапускаем...')
                    procs[name] = run_process([python, name], name)

            now = time.time()

            # Проверяем состояние stations_catalog.py
            if stations_proc is None:
                # Если процесс не запущен, проверяем, можно ли запускать
                if now - stations_last_run >= stations_cooldown:
                    stations_proc = run_process(stations_cmd, 'stations_catalog.py')
                else:
                    remaining = int(stations_cooldown - (now - stations_last_run))
                    logging.debug(f'Ждём {remaining} сек до следующего запуска stations_catalog.py')
            else:
                # Если процесс запущен, проверяем, завершился ли он
                retcode = stations_proc.poll()
                if retcode is not None:
                    logging.info(f'stations_catalog.py завершился с кодом {retcode}')
                    stations_proc = None
                    stations_last_run = now  # фиксируем время завершения

            time.sleep(5)

    except KeyboardInterrupt:
        logging.info('Прерывание, завершаем процессы...')
        for proc in procs.values():
            proc.terminate()
        if stations_proc is not None:
            stations_proc.terminate()

    for proc in procs.values():
        proc.wait()
    if stations_proc is not None:
        stations_proc.wait()

    logging.info('Все процессы завершены')

if __name__ == '__main__':
    main()
