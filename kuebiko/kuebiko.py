import json
import os

import requests
from requests.exceptions import RequestException
from requests.models import HTTPError
from SPARQLWrapper import JSON, SPARQLWrapper

from kuebiko.article_loader.article_loader import ArticleLoader
from multiprocessing import Queue

WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'

class Kuebiko:
    def __init__(self, amount_processes: int = 5) -> None:
        self.amount_processes = amount_processes
        self.sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)
        self.sparql.setReturnFormat(JSON)
        self.queue = Queue()

    def query(self, query_file: str) -> list:
        wikidata_ids = self.load_dataset(query_file)
        batches = self.batch_list(wikidata_ids, self.amount_processes)
        processes = []
        for i in range(self.amount_processes):
            p = ArticleLoader(batches[i], self.queue)
            p.start()
            processes.append(p)
        [p.join() for p in processes]
        # TODO #3 work with output from queue

    def batch_list(self, to_batch: list, amount_batches: int):
        batches = [[] for _ in range(amount_batches)]
        for _ in range(len(to_batch) // amount_batches):
            for i in range(amount_batches):
                batches[i].append(to_batch.pop())
        return batches

    def read_query_file(self, query_file: str) -> dict:
        if not os.path.exists(query_file):
            raise FileNotFoundError('Query file not found at: {}'.format(query_file))

        with open(query_file, 'r', encoding='utf-8') as file:
            return json.load(file)

    def load_dataset(self, query_file: str) -> list:
        query = self.read_query_file(query_file)
        self.sparql.setQuery(query['query'])
        res = self.sparql.query().convert()
        bindings = res.get('results', {}).get('bindings')
        return self.parse_wikidata_ids(bindings)

    def parse_wikidata_ids(self, bindings: list) -> list:
        wikidata_ids: list = []
        for binding in bindings:
            uri = binding['cid']['value']
            wikidata_ids.append(uri.split('/').pop())
        return wikidata_ids
