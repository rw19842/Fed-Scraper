from fed_scraper.spiders.beige_book_archive import BeigeBookArchiveSpider
from datetime import datetime


class BeigeBookCurrentSpider(BeigeBookArchiveSpider):
    name = "beige_book_current"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = [
        "https://www.federalreserve.gov/monetarypolicy/publications/beige-book-default.htm"
    ]

    def __init__(self):
        self.current_year = str(datetime.now().year)

    def parse(self, response):
        yield response.follow(
            response.request.url,
            callback=self.parse_year_page,
            cb_kwargs={"year": self.current_year},
        )
