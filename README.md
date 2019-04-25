# Advance Scraping: Scheduling and Storing Data

Using celery, mongodb and requests to show how to schedule, and store
scraped data.

![Design Flow-ish]( https://github.com/Proteusiq/advance_scraping/blob/master/images/flow_celery.png)


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

Two objectives this repo is attempting to achieve:
* document how to use MongoDB as broker and backend in Celery
* Show how to schedule a continous data scraping

### Prerequisites

Softwares:
   * MongoDB is running on localhost
   * Git and Python 3.6+.
   * Alphavantage API key

[Alphavantage](https://www.alphavantage.co/): Get a free API key


### Installing

Clone this repo and create a project environment with pymongo, celery, and requests.
Before running, set your API keys in environment variable called ALPHAVANTAGE

```bash
git clone https://github.com/Proteusiq/advance_scraping.git && cd advance_scraping
conda env create -f environment.yml
conda activate tasks
```

Alternative

```bash
git clone https://github.com/Proteusiq/advance_scraping.git && cd advance_scraping
conda create -n tasks -c conda-forge python=3.7 pymongo celery requests
conda activate tasks
```

How to run:

#### In terminal A [Worker Terminal], run celery's worker

```
cd source
export ALPHAVANTAGE=API_KEYS_HERE
celery -A tasks.app worker --loglevel=info
```

> Note: Windows uses SET ALPHAVANTAGE=API_KEYS_HERE

#### Open another termnial B [Beat Terminal], navigate to the project and run celery's beat(crontab-like scheduler)

```
cd source
conda activate tasks
export ALPHAVANTAGE=API_KEYS_HERE
celery -A tasks.app beat --loglevel=info
```
> Note: Don´t do this in production, use demonization to run process

#### Toy Project: Scrap Stock Data Every 5 Minutes

This project has mainly two python files:
- tasks.py
- mongo_config.py

tasks.py contains a function to get stocks using requests library
and set celery settings. I have commented most lines to help you understand
what they do.

## Results
Two terminals:
![Celery Worker and Beat](https://github.com/Proteusiq/advance_scraping/blob/master/images/celery_in_action.png)

Data in MongoDB
![MongoDB](https://github.com/Proteusiq/advance_scraping/blob/master/images/mongo_results.png)

## Deployment in Production

Working on Docker Image. [Coming soon]

## Built With

* [Celery](http://docs.celeryproject.org/en/latest/index.html) - Run tasks both async and schedue
* [MongoDB](https://docs.mongodb.com/manual/) - DataBase used as broker and backend
* [Requests](https://2.python-requests.org/en/master/) - Used to get stock data from API



## Authors

* **Prayson W. Daniel** - *Initial work* - [Proteusiq](https://github.com/Proteusiq)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
