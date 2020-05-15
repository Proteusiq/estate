'''
    Prototype: To be replaced
'''


from abc import ABC, abstractmethod
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests import Session
import numpy as np
import pandas as pd


class Boliga(ABC):

    def __init__(self, url, headers=None):

        session = Session()

        self.BASE_URL = url

        if headers is None:
            headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                                      'Chrome/39.0.2171.95 Safari/537.36'),
                       'Content-Type': 'application/json'}

        session.headers.update(headers)

        self.session = session
        self.meta_data = None
        self.store = pd.DataFrame()

    def __repr__(self):
        return f'{self.__class__.__name__}(API={repr(self.BASE_URL)})'

    @abstractmethod
    def get_page(self, *args, **kwargs):
        pass

    def get_pages(self, *args, **kwargs):
        pass


class BoligaRecent(Boliga):

    '''
    expects Base URL
    e.g.
        url = 'https://api.boliga.dk/api/v2/search/results'
    '''

    def get_page(self, page=1, pagesize=100, postal='2650', verbose=False):
        '''Gather Data From Boliga API
            page:int page number
            pagesize:int number of boligs in a page
            postal:str zip/postal code of a city
            verbose:bool print mining progress
        '''

        params = {'page': page,
                  'pageSize': pagesize,
                  'zipCodes': postal, }

        r = self.session.get(self.BASE_URL, params=params)

        if r.ok:
            data = r.json()

            self.store = self.store.append(
                pd.DataFrame(data.get('results')), ignore_index=True)
            self.meta_data = data.get('meta')

        else:
            self.store

        if verbose:
            print(f'[+] Gathering data from page {page:}.{" ":>5}Found {len(self.store):>5} estates'
                  f'{" ":>3}Time {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')

        return self

    def get_pages(self, page=1, pagesize=100, postal='2650', verbose=False):
        '''
         Parallel Gathering Data From Boliga
            page:int page number to start
            verbose:bool print mining progress
        '''

        # Make the first call to get total number of pages for split call pagesize split

        self.get_page(page=page, pagesize=pagesize,
                      postal=postal, verbose=verbose)

        total_pages = self.meta_data.get('totalPages')

        # since we got the first page, we can get the rest

        if page <= total_pages:
            page += 1

            self.get_page(page=page,
                            pagesize=pagesize,
                            postal=postal,
                            verbose=verbose)


        return self
        
'''
Example:
CONNECTION_URI = (f"postgresql://{os.getenv('POSTGRES_USER','danpra')}:"
                  f"{os.getenv('POSTGRES_PASSWORD', 'postgrespwd')}@postgres:5432/bolig_db"
                  )
TABLE_NAME = 'recent_bolig'

bolig = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')
bolig.get_pages(postal='2600',verbose=True)

engine = sqlalchemy.create_engine(CONNECTION_URI)
bolig.to_sql(bolig.store, engine, if_exists='append')
engine.dispose()

'''
