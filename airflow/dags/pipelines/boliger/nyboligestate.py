from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import pandas as pd



from pipelines.boliger.bolig import Bolig

# Building API to Nybolig | Estate 
 # Requirements: 
 #      Overide get_page and get_pages.
 #      get_page: Task: Update params and requests GET|POST logic in get_pages
 #      get_pages: Task: Update total pages logic only      

class Services(Bolig):
    
    '''
    Bolig Data From nybolig.dk and estate.dk

    Network:
        Request URL: https://www.nybolig.dk/Services/PropertySearch/Search
        Request URL: https://www.estate.dk/Services/PropertySearch/Search

    POST LOAD:
            'isRental': False,
            'mustBeSold': False,
            'mustBeInProgress': False,
            'siteName': 'Estate|Nybolgig',
            'take': 100,
            'skip': 0,
            'sort': 0

    Usage:
    ```python
    # instantiate a class. Services is inhereted by Nybolig(Services) | Estate(Serices)
    nybolig = Nybolig(url='https://www.nybolig.dk/Services/PropertySearch/Search')
    estate = Estate(url='https://www.estate.dk/Services/PropertySearch/Search')
    
    # one page per call 
    print('[+] Nybolig | Start single thread calls\n')
    _ = {nybolig.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

    # one page per call 
    print('[+] Estate | Start single thread calls\n')

    _ = {estate.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

    ## store data to df
    df = estate.store
    print(f'Data Stored {df.shape[0]} rows from estate.dk\n')
    print(f'Data Stored {nybolig.store.shape[0]} rows from nybolig.dk\n')


    # multipe pages per call
    workers = 6
    print(f'[+] Start {workers} threads calls\n')
    estate.get_pages(start_page=6,end_page=10, pagesize=200, workers=6, verbose=True)
    dt = estate.store
    print(dt.dtypes) # data types
    ```    
    '''    


    def get_page(self, page=0, pagesize=100 , verbose=False, **kwargs):
        '''Gather Data From Estate | Nybolig API
            page:int page number. default value 0
            pagesize:int number of boligs in a page. default value 100
            verbose:bool print mining progress. default value False
        '''
        
        take = pagesize
        skip = pagesize*page
        
        # www.[souce].dk/api ...
        source = self.BASE_URL.split('.')[1].title()

        params = {'isRental': False,
                    'mustBeSold': False,
                    'mustBeInProgress': False,
                    'siteName': source,
                    'take': take,
                    'skip': skip,
                    'sort': 0
                }

        params.update(kwargs)

        r = self.session.post(self.BASE_URL, data=dict(params))
       
        if r.ok:
            data = r.json()
            
            self.store = self.store.append(
                    pd.DataFrame(data.get('Results')), ignore_index=True)
            self.max_pages = data.get('TotalAmountOfResults')

        else:
            self.store
            
        if verbose:
            print(f'[+] Gathering data from page {page:}.{" ":>5}Found {len(self.store):>5} estates'
                 f'{" ":>3}Time {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')

        return self

    
    def get_pages(self, start_page=0, end_page=None, pagesize=100, workers=4, verbose=False, **kwargs):
        '''
         Parallel Gathering Data From Home
            start_page:int page number to start. default value 0
            end_page:int page number to stop. default value None
            pagesize:int number of boligs per page. default valeu 100
            verbose:bool print mining progress. default value False
        '''
        
        # Make the first call to get total number of pages for split call pagesize split
        # TODO: Pass kwargs
        self.get_page(page=start_page, pagesize=pagesize, verbose=verbose)
        
        if end_page is None:
            total_pages = self.max_pages/pagesize
        else:
            total_pages = start_page + end_page + 1
        
        # since we got the first page, we can get the rest
        
        if start_page <= total_pages:
            start_page += 1

            func = lambda pages: {self.get_page(page, pagesize, verbose=verbose) for page in pages}
            pages_split = np.array_split(np.arange(start_page,total_pages+1), workers)
        
            with ThreadPoolExecutor(max_workers=min(32,workers)) as executor:
                _ = {executor.submit(func,split) for split in pages_split}
        
        return self


class Estate(Services):
    pass

class Nybolig(Services):
    pass
