Тест парсера  
Запуск - run_parser.py  
Все необходимые библиотеки - requirements.txt  
pip install -r requirements.txt  
Настроить Redis в config.json

docker exec -it redis redis-cli DEL space_weather:last_modified space_weather:etag file_queue  

