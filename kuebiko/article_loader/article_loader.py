from kuebiko import article_loader
from multiprocessing import Process, Queue
import requests
from requests.exceptions import RequestException
from requests.models import HTTPError
import time

RETRY_SLEEP = 2.0
MAX_RETRIES = 10
WIKIDATA_JSON_ENDPOINT = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=<<WIKIDATA_ID>>'

class ArticleLoader(Process):
    def __init__(self, wikidata_ids: list, queue: Queue):
        super(ArticleLoader, self).__init__()
        self.wikidata_ids = wikidata_ids
        self.queue = queue
        self._tmp_retries: int = 0

    def download_article(self, wikidata_id: str) -> dict:
        try:
            url = WIKIDATA_JSON_ENDPOINT.replace('<<WIKIDATA_ID>>', wikidata_id)
            response = requests.get(url)
            if response.status_code != 200:
                raise HTTPError(response.text)
            if 'error' in response.json().keys():
                raise RequestException(response.text)
            return response.json()['entities'][wikidata_id]
        except:
            if self._tmp_retries <= MAX_RETRIES:
                print('Error with {} -> retrying after {} seconds.'.format(wikidata_id, RETRY_SLEEP))
                time.sleep(RETRY_SLEEP)
                self._tmp_retries += 1
                return self.download_article(wikidata_id)
            else:
                print('Error with {} -> exceeded {} retries -> returning None'.format(wikidata_id, MAX_RETRIES))
                return None

        
    def run(self):
        for wikidata_id in self.wikidata_ids:
            retry_count: int = 0
            article: dict = self.download_article(wikidata_id)
            # TODO #2 pre process article
            self.queue.put(article)
