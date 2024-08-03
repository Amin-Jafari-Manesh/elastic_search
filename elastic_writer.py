import logging
from elasticsearch import Elasticsearch
from os import environ

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'HASH_SIZE': environ.get('HASH_SIZE', ''),
    'RECORDS': environ.get('RECORDS', ''),
}


def generate_random_hash(numb: int = 1) -> str:
    import random
    import string
    import hashlib
    if numb == 1:
        return hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
    else:
        return ''.join(
            [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
             for _ in range(numb)])


def check_elastic_search_connection():
    elastic_search = Elasticsearch(f'{db_config["DOMAIN"]}', basic_auth=('elastic', db_config['PASS']))
    elastic_ping = elastic_search.ping()
    if elastic_ping:
        logging.info(f"Successfully connected to ElasticSearch: {elastic_ping}")
        return True
    else:
        logging.error(f"Failed to connect to ElasticSearch: {elastic_ping}")
        return False


def elastic_search_write_hash(size: int = 100) -> bool:
    if check_elastic_search_connection():
        es = Elasticsearch(f'{db_config["DOMAIN"]}', basic_auth=('elastic', db_config['PASS']))
        for _ in range(size):
            es.index(index='hashes', body={'hash': generate_random_hash(db_config['HASH_SIZE'])})
        return True
    return False
