# Manually trigger a task
# Used in testing the behavior of API, MongoDB, and Celery
import time

from datetime import datetime
from os import environ

from tasks import get_stock

def run_stock(params:dict,use_celery:bool=True):
      
      
      start_time = datetime.now()
      if use_celery: # Using celery
            response = get_stock.delay(params)
      else: # Not using celery
            response = get_stock(params)

      print(f'It took {datetime.now()-start_time}!'
            ' to run')
      print(f'response: {response}')

      return response

if __name__ == '__main__':
      
      # Call using celery
      PARAMS = {'function':'TIME_SERIES_INTRADAY',
      'symbol':'MSFT',
      'interval':'5min',
      'apikey': environ['ALPHAVANTAGE'] ,
      }
      response = run_stock(params=PARAMS)
      while not response.ready():
            print(f'[Waiting] It is {response.ready()} that we have results')
            time.sleep(5) # sleep to second
      print('\n We got results:')
      print(response.result)