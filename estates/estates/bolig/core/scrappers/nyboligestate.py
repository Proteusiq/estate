from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import pandas as pd
from estates.bolig.io.lazylogger import logger
from estates.bolig.core.scrap import Bolig

# Building API to Nybolig | Estate
# Requirements:
#      Overide get_page and get_pages.
#      get_page: Task: Update params and requests GET|POST logic in get_pages
#      get_pages: Task: Update total pages logic only


class Services(Bolig):

    """
    Bolig Data From nybolig.dk and estate.dk

    Network:
        Request URL: https://www.nybolig.dk/Services/PropertySearch/Search
        Request URL: https://www.estate.dk/Services/PropertySearch/Search

    POST LOAD:
            'isRental': False,
            'mustBeSold': False,
            'mustBeInProgress': False,
            'siteName': 'Estate|Nybolig',
            'take': 20, # max 20
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
    df = pd.concat(estate.store.values(), ignore_index=True)
    print(f'Data Stored {df.shape[0]} rows from estate.dk\n')
    print(f'Data Stored {pd.concat(nybolig.store.values(), ingore_index=True).shape[0]} rows from nybolig.dk\n')


    # multipe pages per call
    workers = 6
    print(f'[+] Start {workers} threads calls\n')
    estate.get_pages(start_page=6,end_page=10, pagesize=200, workers=6, verbose=True)
    dt = estate.DataFrame
    print(dt.dtypes) # data types
    ```
    """

    def get_page(self, page=1, pagesize=20, verbose=False, **kwargs):
        """Gather Data From Estate | Nybolig API
            page:int page number. default value 0
            pagesize:int number of boligs in a page. default value 100
            verbose:bool print mining progress. default value False

        Returns: self.store: list of DataFrame
        """

        if pagesize > 20:
            logger.warn(f"[+] Maxing page size is 20. Given {pagesize}!")

        take = pagesize
        skip = pagesize * page

        # www.[souce].dk/api ...
        source = self.BASE_URL.split(".")[1].title()

        params = {
            "isRental": False,
            "mustBeSold": False,
            "mustBeInProgress": False,
            "siteName": source,
            "take": take,
            "skip": skip,
            "sort": 0,
        }

        params.update(kwargs)

        r = self.session.post(self.BASE_URL, data=dict(params))

        if r.ok:
            data = r.json()

            self.store[page] = pd.DataFrame(data.get("Results"))
            self.max_pages = int(np.ceil(data.get("TotalAmountOfResults") / take))

        else:
            self.store

        if verbose:
            logger.info(
                f'[+] Gathering data from page {page:}.{" ":>5}Found {len(self.store) * take:>5} estates'
                f'{" ":>3}Time {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}'
            )

        return self

    def get_pages(
        self,
        start_page=1,
        end_page=None,
        pagesize=20,
        workers=4,
        verbose=False,
        **kwargs,
    ):
        """
         Parallel Gathering Data From Home
            start_page:int page number to start. default value 0
            end_page:int page number to stop. default value None
            pagesize:int number of boligs per page. default valeu 100
            verbose:bool print mining progress. default value False

        Returns: self.DataFrame
        """

        # Make the first call to get total number of pages for split call pagesize split
        # TODO: Pass kwargs
        self.get_page(page=start_page, pagesize=pagesize, verbose=verbose)

        if end_page is None:
            end_page = self.max_pages + 1

        # since we got the first page, we can get the rest
        start_page += 1
        if start_page <= end_page:

            pages_split = np.array_split(np.arange(start_page, end_page), workers)

            with ThreadPoolExecutor(max_workers=min(32, workers)) as executor:
                _ = {
                    executor.submit(
                        lambda pages: {self.get_page(page, pagesize, verbose=verbose) for page in pages}, split
                    )
                    for split in pages_split
                }

        if len(self.store):
            self.DataFrame = pd.concat(self.store.values(), ignore_index=True)
        else:
            self.DataFrame = pd.DataFrame([])

        return self


class Estate(Services):
    pass


class Nybolig(Services):
    pass
