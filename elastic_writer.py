import logging
from elasticsearch import Elasticsearch
from os import environ
import time

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'DATA_SIZE': int(environ.get('HASH_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', '')),
}

elastic_search = Elasticsearch(f'http://{db_config["DOMAIN"]}:9200', basic_auth=('elastic', db_config['PASS']))


def generate_random_hash(numb: int = 1) -> str:
    import random
    import string
    import hashlib
    return ''.join(
        [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
         for _ in range(numb)])


def generate_text(numb: int) -> str:
    text = ' The quick brown fox jumps over the lazy dog today. '
    return ''.join([text for _ in range(numb)])


def elastic_search_write(size: int = 100) -> bool:
    if not elastic_search.ping():
        logging.error("Failed to connect to the database.")
        return False

    if db_config['DATA_TYPE'] == 'h':
        index_name = 'hashes'
        func = generate_random_hash
    elif db_config['DATA_TYPE'] == 't':
        index_name = 'texts'
        func = generate_text
    else:
        logging.error("Invalid data type.")
        return False
    try:
        for _ in range(size):
            time.sleep(db_config['INSERT_DELAY'] / 1000)
            elastic_search.index(index=f'{index_name}', body={f'{index_name[-1]}': func(db_config['DATA_SIZE'])})
    except Exception as e:
        logging.error(f"Error writing to the database: {e}")
        return False

    return True


if __name__ == '__main__':
    if elastic_search_write(db_config['RECORDS']):
        logging.info("Data successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
