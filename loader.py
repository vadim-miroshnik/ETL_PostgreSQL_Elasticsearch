from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor
from contextlib import closing
import backoff
import requests
from state import BaseStorage, JsonFileStorage, State
from sql import SQL

def create_index(client):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index="movies",
        body={
             "settings": {"refresh_interval": "1s",
             "analysis": {"filter": {"english_stop": {"type": "stop", "stopwords": "_english_"},
                                    "english_stemmer": {"type": "stemmer", "language": "english"},
                                    "english_possessive_stemmer": {"type":"stemmer","language": "possessive_english"},
                                    "russian_stop": {"type": "stop","stopwords": "_russian_"},
                                    "russian_stemmer": {"type": "stemmer","language": "russian"}},
            "analyzer": {"ru_en": {"tokenizer": "standard","filter": ["lowercase","english_stop",
            "english_stemmer","english_possessive_stemmer","russian_stop","russian_stemmer"]}}}},
            "mappings": {"dynamic": "strict",
            "properties": {
            "id": {"type": "keyword"},
            "imdb_rating": {"type": "float"},
            "genre": {"type": "keyword"},
            "title": {"type": "text","analyzer": "ru_en","fields": {"raw": { "type":  "keyword"}}},
            "description": {"type": "text","analyzer": "ru_en"},
            "director": {"type": "text","analyzer": "ru_en"},
            "actors_names": {"type": "text","analyzer": "ru_en"},
            "writers_names": {"type": "text","analyzer": "ru_en"},
            "actors": {"type": "nested","dynamic": "strict","properties": 
                        {"id": {"type": "keyword"},"name": {"type": "text","analyzer": "ru_en"}}},
            "writers": {"type": "nested","dynamic": "strict","properties": 
                        {"id": {"type": "keyword"},"name": {"type": "text","analyzer": "ru_en"}}}
            }}},ignore=400,)

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError))
def es_connect():
    return Elasticsearch(os.environ.get('ES_HOST_PORT'))

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError))
def pg_connect():
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}    
    return closing( psycopg2.connect(**dsl, cursor_factory=DictCursor))

def generate_actions(storage: BaseStorage):
    PAGE_SIZE = 100
    load_dotenv()
    stateman = State(storage=storage)
    default_date = str(datetime(year=2021, month=5, day=1))
    current_state = stateman.state.get("filmwork", default_date)
    with pg_connect() as pg_conn:
        cur = pg_conn.cursor()
        cur.execute(SQL, (current_state,))
        while True:
            rows = cur.fetchmany(PAGE_SIZE)
            if not rows:
                break
            for row in rows:
                doc = {
                    "_index": "movies",
                    "_id": row[0],
                    "id": row[0],
                    "imdb_rating": row[3],
                    "genre": [g for g in row[8]],
                    "title": row[1],
                    "description": row[2],
                    "director": ','.join([act['person_name'] for act in row[7] if act['person_role'] == 'director']),
                    "actors_names": ','.join([act['person_name'] for act in row[7] if act['person_role'] == 'actor']),
                    "writers_names": [act['person_name'] for act in row[7] if act['person_role'] == 'writer'],
                    "actors": [dict(id=act['person_id'],name=act['person_name']) for act in row[7] if act['person_role'] == 'actor'],
                    "writers": [dict(id=act['person_id'],name=act['person_name']) for act in row[7] if act['person_role'] == 'writer'],
                }
                yield doc
            stateman.set_state(key="filmwork", value=str(rows[-1][6]))


if __name__ == '__main__':
    es = es_connect()
    create_index(es)
    successes = 0
    storage = JsonFileStorage('filmwork_state.json')
    for ok, action in streaming_bulk(
        client=es, actions=generate_actions(storage),
    ):
        successes += 1
    print(successes)