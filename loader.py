import logging
import os
from contextlib import closing
from datetime import datetime
from http import HTTPStatus

import backoff
import psycopg2
import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from psycopg2.extras import DictCursor

from settings import INDEX_NAME, INDEX_SETTINGS
from sql import SQL
from state import BaseStorage, JsonFileStorage, State


def create_index(client):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index=INDEX_NAME,
        body=INDEX_SETTINGS,
        ignore=HTTPStatus.BAD_REQUEST,)

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

def transform(row):
    return {
        "_index": "movies",
        "_id": row['id'],
        "id": row['id'],
        "imdb_rating": row['rating'],
        "genre": [g for g in row['genres']],
        "title": row['title'],
        "description": row['description'],
        "director": ','.join([act['person_name'] for act in row['persons'] if act['person_role'] == 'director']),
        "actors_names": ','.join([act['person_name'] for act in row['persons'] if act['person_role'] == 'actor']),
        "writers_names": [act['person_name'] for act in row['persons'] if act['person_role'] == 'writer'],
        "actors": [dict(id=act['person_id'],name=act['person_name']) for act in row['persons'] if act['person_role'] == 'actor'],
        "writers": [dict(id=act['person_id'],name=act['person_name']) for act in row['persons'] if act['person_role'] == 'writer'],
    }

def generate_actions(storage: BaseStorage):
    PAGE_SIZE = 100
    load_dotenv()
    stateman = State(storage=storage)
    default_date = str(datetime(year=2021, month=5, day=1))
    current_state = stateman.state.get("filmwork", default_date)
    with pg_connect() as pg_conn:
        cur = pg_conn.cursor(cursor_factory=DictCursor)
        cur.execute(SQL, (current_state,current_state,current_state,))
        while True:
            rows = cur.fetchmany(PAGE_SIZE)
            if not rows:
                break
            for row in rows:
                yield transform(row)
            stateman.set_state(key="filmwork", value=str(rows[-1]['modified']))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    es = es_connect()
    create_index(es)
    successes = 0
    storage = JsonFileStorage('filmwork_state.json')
    for ok, action in streaming_bulk(
        client=es, actions=generate_actions(storage),
    ):
        successes += 1
    logging.info(f"{successes} records added to index")