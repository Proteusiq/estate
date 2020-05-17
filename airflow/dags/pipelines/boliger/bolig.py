'''
PyData Example: Advance Web Scraping
        Build API for end-user One Class to Rule them all 
        Note: Use for educational purposes only
Using Design Patterns
        Singleton Design Pattern
        Abstract Factory Pattern
'''

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import pandas as pd
from requests import Session



class Bolig(ABC):
    """Global point of access to all Bolig related classes
        The abstract class to which bolig classes will be built on by
        overiding only get_page and get_pages method
    """

    def __init__(self, url, headers=None):
        
        session = Session()

        self.BASE_URL = url
        

        if headers is None:
            headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/39.0.2171.95 Safari/537.36'),
            }

        session.headers.update(headers)
        self.session = session
        self.meta_data = None
        self.store =  pd.DataFrame()

    
    def __repr__(self):
        return f'{self.__class__.__name__}(API={repr(self.BASE_URL)})'

    
    @abstractmethod
    def get_page(self, *args, **kwargs):
        pass
    
    @abstractmethod
    def get_pages(self, *args, **kwargs):
        pass
