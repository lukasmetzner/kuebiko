import json
import os
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from requests.exceptions import RequestException
from requests.models import HTTPError

WIKIDATA_JSON_ENDPOINT = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=<<WIKIDATA_ID>>'
WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'

class Kuebiko:
    def __init__(self) -> None:
        self.sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)
        self.sparql.setReturnFormat(JSON)

    def query(self, query_file: str) -> list:
        dataset = self.load_dataset(query_file)
        return self.download_wikidata_articles(dataset)

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

    def download_article(self, wikidata_id: str) -> dict:
        url = WIKIDATA_JSON_ENDPOINT.replace('<<WIKIDATA_ID>>', wikidata_id)
        response = requests.get(url)
        if response.status_code != 200:
            raise HTTPError(response.text)
        if 'error' in response.json().keys():
            raise RequestException(response.text)
        return response.json()['entities'][wikidata_id]

    def download_wikidata_articles(self, wikidata_ids: list) -> list:
        articles = []
        for wikidata_id in wikidata_ids:
            article = self.download_article(wikidata_id)
            articles.append(article)
        return articles