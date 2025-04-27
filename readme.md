Тест парсера  
Запуск - run_parser.py  
Все необходимые библиотеки - requirements.txt  
pip install -r requirements.txt  
Настроить Redis в config.json
run_parser.py запускает параллельно stations_catalog.py (проверяет все существующие станции на принадлежность к России),  
downloader.py (Загружает необходимые файлы),   
parser_worker.py (Парсит скаченные файлы, переводя в СИ)  
