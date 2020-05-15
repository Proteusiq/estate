'''
Main example of using the design
    Note: Use for educational purposes only
'''

from pipelines.boliger import BoligaRecent
from pipelines.boliger import Home
from pipelines.boliger import Nybolig, Estate


# Home example
api_name = 'home.dk'

print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')

 # instantiate a class
homes = Home(url='https://home.dk/umbraco/backoffice/home-api/Search')

# one page per call 
print('[+] Start single thread calls: page 0-6\n')
_ = {homes.get_page(page=page, pagesize=15, verbose=True) for page in range(0,6)}

## store data to df
df = homes.store
print(f'Data gathed {df.shape[0]} rows\n')


# multipe pages per call
workers = 5
start_page = 10
end_page = 25
page_size=15

print(f'[+] Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
homes.get_pages(start_page=start_page, end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
print(f'Data gathered {homes.store.shape[0]} rows\n')





# Boliga example
api_name = 'boliga.dk'
print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')


# instantiate a class
boliga = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')

# one page per call 
print('[+] Start single thread calls: page 0-9\n')
_ = {boliga.get_page(page=page, pagesize=15, verbose=True) for page in range(0,10)}

## store data to df
df = boliga.store
print(f'Data Stored {df.shape[0]} rows\n')


# multipe pages per call
workers = 6
start_page = 6
end_page = 10
page_size = 200

print(f'[+] Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
boliga.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = boliga.store
print(f'\n{dt.shape[0]} rows found. Data types are?')
print(dt.dtypes) # data types


# Home example
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

## store data to df
df = estate.store
print(f'Data Stored {df.shape[0]} rows from estate.dk\n')
print(f'Data Stored {nybolig.store.shape[0]} rows from nybolig.dk\n')


# multipe pages per call
workers = 6
start_page = 6
end_page = 10
page_size = 200

print(f'[+] Estate | Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
estate.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = estate.store
print(dt.dtypes) # data types
print(f'\nData Stored {dt.shape[0]} rows from estate.dk\n')


print(f'[+] Nybolig | Start {workers} threads for {page_size} pagesize per call: start at page {start_page} and at page {end_page} \n')
nybolig.get_pages(start_page=start_page,end_page=end_page, pagesize=page_size, workers=workers, verbose=True)
dt = nybolig.store
print(dt.dtypes) # data types
print(f'\nData Stored {dt.shape[0]} rows from nybolig.dk\n')



print('\n[+] Example Completed Successful :)')