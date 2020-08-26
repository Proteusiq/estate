'''
Main example of using the design
    Note: Use for educational purposes only
'''
import pandas as pd # Needed for single threads
from pipelines.boliger import BoligaRecent
from pipelines.boliger import BoligaSold
from pipelines.boliger import Estate
from pipelines.boliger import Home
from pipelines.boliger import Nybolig



# Home example
api_name = 'home.dk'

print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')

 # instantiate a class
homes = Home(url='https://home.dk/umbraco/backoffice/home-api/Search')

# one page per call 
print('[+] Start single thread calls: page 0-6\n')
_ = {homes.get_page(page=page, pagesize=15, verbose=True) for page in range(0,6)}

## stores data to df
df = pd.concat(homes.store.values(), ignore_index=True)
print(f'Data gathed {df.shape[0]} rows\n')


# multipe pages per call
workers = 5
start_page = 10
end_page = 25
page_size=15

print(f'[+] Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
homes.get_pages(start_page=start_page, end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
print(f'Data gathered {homes.DataFrame.shape[0]} rows\n')





# Boliga example
api_name = 'boliga.dk'
print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas from recent priced estates\n')


# instantiate a class
boliga_recent = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')

# one page per call 
print('[+] Start single thread calls: page 0-9\n')
_ = {boliga_recent.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

## stores data to df
df =  pd.concat(boliga_recent.store.values(), ignore_index=True)
print(f'Data storesd {df.shape[0]} rows\n')


# multipe pages per call
workers = 6
start_page = 6
end_page = 10
page_size = 200

print(f'[+] Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
boliga_recent.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = boliga_recent.DataFrame
print(f'\n{dt.shape[0]} rows found. Data types are?')
print(dt.dtypes) # data types


# Bolig Sold
api_name = 'boliga.dk'
print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas for sold estates\n')


# instantiate a class
boliga_sold = BoligaSold(url='https://api.boliga.dk/api/v2/sold/search/results')

# one page per call
print('[+] Start single thread calls: page 0-9\n')
_ = {boliga_sold.get_page(page=page, pagesize=100, verbose=True)
     for page in range(0, 10)}

## stores data to df
df =  pd.concat(boliga_sold.store.values(), ignore_index=True)
print(f'Data storesd {df.shape[0]} sold estates\n')


# multipe pages per call
workers = 13
start_page = 6
end_page = 20
page_size = 200

print(f'[+] Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
boliga_sold.get_pages(start_page=start_page, end_page=end_page,
                        pagesize=page_size, workers=workers, verbose=True)
dt = boliga_sold.DataFrame
print(f'\n{dt.shape[0]} estates found. Data types are?')
print(dt.dtypes)  # data types


# Estate and Nybolig
api_name = 'estate.dk and nybolig.dk'

print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')
# instantiate a class
nybolig = Nybolig(url='https://www.nybolig.dk/Services/PropertySearch/Search')
estate = Estate(url='https://www.estate.dk/Services/PropertySearch/Search')

# one page per call 
print('[+] Nybolig | Start single thread calls. page 0-9\n')
_ = {nybolig.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

# one page per call 
print('[+] Estate | Start single thread calls page 0-9\n')

_ = {estate.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

## stores data to df

df = pd.concat(estate.store.values(), ignore_index=True)
print(f'Data storesd {df.shape[0]} rows from estate.dk\n')
print(f'Data storesd {pd.concat(nybolig.store.values(), ignore_index=True).shape[0]} rows from nybolig.dk\n')


# multipe pages per call
workers = 6
start_page = 6
end_page = 10
page_size = 200

print(f'[+] Estate | Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
estate.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = estate.DataFrame
print(dt.dtypes) # data types
print(f'\nData storesd {dt.shape[0]} rows from estate.dk\n')


print(f'[+] Nybolig | Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
nybolig.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = nybolig.DataFrame
print(dt.dtypes) # data types
print(f'\nData storesd {dt.shape[0]} rows from nybolig.dk\n')



print('\n[+] Example Completed Successful :)')
