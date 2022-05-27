from estates.bolig.io.lazylogger import logger  # noqa


class ScrapEstate:
    def __init__(self, url, api_name, scraper_cls, params, *args, **kwargs):
        self.url = url
        self.api_name = api_name
        self.scraper_cls = scraper_cls
        self.params = params

    def execute(self):

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

        logger.info(f"Data gathered {bolig.DataFrame.shape[0]} rows\n")
        return self.params
