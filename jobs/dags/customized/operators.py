# TODO:
# remove aove add data to intermiate minio

from airflow.models import BaseOperator
from helpers.data_loader import send_bolig  # noqa
from helpers.loggers import logger


class ScrapEstateOperator(BaseOperator):
    template_fields = ("url", "api_name", "scraper_cls", "params")

    def __init__(self, url, api_name, scraper_cls, params, *args, **kwargs):
        super(ScrapEstateOperator, self).__init__(*args, **kwargs)
        self.url = url
        self.api_name = api_name
        self.scraper_cls = scraper_cls
        self.params = params

    def execute(self, context):

        logger.info(
            f"\n[+] Using {self.api_name} to demostrate advance web scraping ideas\n"
        )

        # instantiate a class
        bolig = self.scraper_cls(url=self.url)

        logger.info(
            f'[+] Start {self.params["workers"]} threads for {self.params["pagesize"]} pagesize per call: '
            f'start at page {self.params["start_page"]} and at page {self.params["end_page"]} \n'
        )
        bolig.get_pages(**self.params)
        # homes.DataFrame.drop(columns=['floorPlan', 'pictures'], inplace=True)

        send_bolig(bolig.DataFrame, f"{self.api_name.split('.')[0]}")  # refactor this
        logger.info(f"Data gathered {bolig.DataFrame.shape[0]} rows\n")
        return self.params
