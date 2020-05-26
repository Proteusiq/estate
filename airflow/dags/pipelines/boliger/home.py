from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import pandas as pd
from pipelines.boliger.bolig import Bolig

# Building API to Home.dk  
 # Requirements: 
 #      Overide get_page and get_pages.
 #      get_page: Task: Update params and requests GET|POST logic in get_pages
 #      get_pages: Task: Update total pages logic only      

class Home(Bolig):
    
    '''
    Bolig Data From Home.dk API

    Network:
        Request URL: https://home.dk/umbraco/backoffice/home-api/Search?CurrentPageNumber=2&SearchResultsPerPage=100

    Usage:
    ```python
    # instantiate a class
    homes = Home(url='https://home.dk/umbraco/backoffice/home-api/Search')
    
    # one page per call 
    print('[+] Start single thread calls\n')
    _ = {homes.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

    ## store data to df
    df = pd.concat(homes.store.values(), ignore_index=True)
    print(f'Data Stored {df.shape[0]} rows\n')


    # multipe pages per call
    workers = 5
    print(f'[+] Start {workers} threads calls\n')
    homes.get_pages(start_page=10, end_page=25, pagesize=15, workers=workers, verbose=True)
    df = homes.DataFrame
    print(df.head())
    ```    
    '''

    def get_page(self, page=0, pagesize=100 ,verbose=False):
        '''Gather Data From Home API
            page:int page number. default value 0
            pagesize:int number of boligs in a page. default value 100
            verbose:bool print mining progress. default value False

        Returns: self.store: list of DataFrame
        '''
        
        params = {'CurrentPageNumber':page,
                 'SearchResultsPerPage':pagesize,
                 }
        

        r = self.session.get(self.BASE_URL, params=params)

        if r.ok:
            data = r.json()
     
            self.store[page] = pd.DataFrame(data.get('searchResults'))
            self.max_pages = loops = np.ceil(
                                data['totalSearchResults']/data['searchResultsPerPage']
                            ).astype(int)

        else:
            self.store
            
        if verbose:
            print(f'[+] Gathering data from page {page:}.{" ":>5}Found {len(self.store)*pagesize:>5} estates'
                 f'{" ":>3}Time {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')

        return self

    
    def get_pages(self, start_page=0, end_page=None, pagesize=100, workers=4, verbose=False):
        '''
         Parallel Gathering Data From Home
            start_page:int page number to start. default value 0
            end_page:int page number to stop. default value None
            pagesize:int number of boligs per page. default valeu 100
            verbose:bool print mining progress. default value False

        Returns: self.DataFrame
        '''
        
        # Make the first call to get total number of pages for split call pagesize split
        
        self.get_page(page=start_page, pagesize=pagesize, verbose=verbose)
        
        if end_page is None:
            total_pages = self.max_pages
        else:
            total_pages = start_page + end_page + 1
        
        # since we got the first page, we can get the rest
        
        if start_page <= total_pages:
            start_page += 1

            func = lambda pages: {self.get_page(page, pagesize, verbose=verbose) for page in pages}
            pages_split = np.array_split(np.arange(start_page,total_pages+1), workers)
        
            with ThreadPoolExecutor(max_workers=min(32,workers)) as executor:
                _ = {executor.submit(func,split) for split in pages_split}
        
        if len(self.store):
            self.DataFrame = pd.concat(self.store.values(), ignore_index=True)
        else:
            self.DataFrame = pd.DataFrame([])

        return self