from kuebiko import article_loader
from multiprocessing import Process, Queue
import requests
from requests.exceptions import RequestException
from requests.models import HTTPError

WIKIDATA_JSON_ENDPOINT = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=<<WIKIDATA_ID>>'

class ArticleLoader(Process):
    def __init__(self, wikidata_ids: list, queue: Queue):
        super(ArticleLoader, self).__init__()
        self.wikidata_ids = wikidata_ids
        self.queue = queue

    def download_article(self, wikidata_id: str) -> dict:
        url = WIKIDATA_JSON_ENDPOINT.replace('<<WIKIDATA_ID>>', wikidata_id)
        response = requests.get(url)
        if response.status_code != 200:
            raise HTTPError(response.text)
        if 'error' in response.json().keys():
            raise RequestException(response.text)
        return response.json ()['entities'][wikidata_id]
        
    def run(self):
        for wikidata_id in self.wikidata_ids:
            article: dict
            try:
                article = self.download_article(wikidata_id)
            except:
                # TODO #1 retry multiple times on error
                print('Error with {}'.format(wikidata_id))
            # TODO #2 pre process article
            self.queue.put(article)