from datetime import datetime
from os import environ

from requests import Request, Session

from celery import Celery
from celery.schedules import crontab


# Setting for celery MongoDB as Backend and Broker
BROKER_URL = 'mongodb://localhost:27017/stocks'
app = Celery('tasks',broker=BROKER_URL)

#Load Backend Settings
app.config_from_object('mongo_config')
# disable UTC. Use local time
app.conf.enable_utc = False

# Settings for getting data
URL = 'https://www.alphavantage.co/query'
HEADERS = {"Content-Type": "application/json", 
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                       " AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/73.0.3683.103 Safari/537.36",
            }

session = Session()
session.headers.update(HEADERS)


@app.task
def get_stock(params:dict):
    '''return stock:
        example of param:
    
    >>> PARAMS = {
    'function':'TIME_SERIES_INTRADAY',
     'symbol':'MSFT',
    'interval':'5min',
    'apikey':'demo',
    }

    '''

    response = session.get(URL,params=params)
    if response.ok:
        return {'status':'success', 
                'insert_tms':datetime.now(),
                'data': response.json(),}
    else:
        return {'status':'failed', 
                'insert_tms':datetime.now(),
                'data':None,}

# To run tasks
# celery -A tasks.app worker --loglevel=info


# Set a cron-like Job
PARAMS = {'function':'TIME_SERIES_INTRADAY',
      'symbol':'MSFT',
      'interval':'5min',
      'apikey': environ['ALPHAVANTAGE'] ,
      }
# add "get_stock" task to the beat schedule
app.conf.beat_schedule = {
    'get_stock-task': {
        'task': 'tasks.get_stock',
        'schedule': crontab(minute='*/5'), #every 5 minutes
        'args': [PARAMS,],
        
    }
}

# To run cron-like
# celery -A tasks.app beat --loglevel=info