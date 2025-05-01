import redis
import logging

def get_redis_client(config):
    return redis.Redis(
        host=config['redis_host'],
        port=config['redis_port'],
        db=config['redis_db']
    )

def blpop_file(redis_client, queue_name, timeout=5):
    try:
        item = redis_client.blpop(queue_name, timeout=timeout)
        if item:
            return item[1].decode()
        return None
    except Exception as e:
        logging.error(f'Ошибка при работе с Redis: {e}')
        return None
