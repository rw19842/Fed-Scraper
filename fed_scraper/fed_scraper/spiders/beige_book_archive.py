import scrapy
from fed_scraper.items import FedScraperItem, serialize_url
from fed_scraper.parse_pdf import parse_pdf_from_url
import re

MEETING_DATES_FILE_PATH = "../meeting_dates.csv"


class BeigeBookArchiveSpider(scrapy.Spider):
    name = "beige_book_archive"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = [
        "https://www.federalreserve.gov/monetarypolicy/beige-book-archive.htm"
    ]
    custom_settings = {
        "ITEM_PIPELINES": {
            "fed_scraper.pipelines.DuplicateUrlPipeline": 50,
            "fed_scraper.pipelines.TextPipeline": 100,
            "fed_scraper.pipelines.MeetingDatesPipeline": 150,
            "fed_scraper.pipelines.RemoveMissingPipeline": 175,
            "fed_scraper.pipelines.CsvPipeline": 200,
            "fed_scraper.pipelines.SortByMeetingDatePipeline": 250,
            "fed_scraper.pipelines.DuplicatesPipeline": 300,
            "fed_scraper.pipelines.SplitCsvPipeline": 400,
        }
    }

    def parse(self, response):
        anchors = response.css(".panel-body a")
        for anchor in anchors:
            year = anchor.css("::text").get()
            year_page_url = anchor.css("::attr(href)").get()
            yield response.follow(
                year_page_url,
                callback=self.parse_year_page,
                cb_kwargs={"year": year},
            )

    def parse_year_page(self, response, year):
        rows = response.css("tbody tr")
        for row in rows:
            date_str = " ".join([row.css("td *::text").get(), year])
            beige_book = FedScraperItem(
                document_kind="beige_book",
                release_date=date_str,
            )
            anchors = row.css("a")
            for anchor in anchors:
                if bool(re.search(r"PDF", anchor.css("::text").get(), re.I)):
                    beige_book["url"] = anchor.css("::attr(href)").get()
                    break
                elif bool(re.search(r"HTML", anchor.css("::text").get(), re.I)):
                    beige_book["url"] = anchor.css("::attr(href)").get()

            if bool(re.search(r".pdf", beige_book.get("url"))):
                beige_book["text"] = parse_pdf_from_url(
                    serialize_url(beige_book.get("url"))
                )
                yield beige_book
            elif bool(re.search(r".htm", beige_book.get("url"))):
                yield response.follow(
                    beige_book["url"],
                    callback=self.parse_html_beige_book,
                    cb_kwargs={"beige_book": beige_book},
                )

    def parse_html_beige_book(self, response, beige_book):
        beige_book["text"] = response.css("#article *::text").getall()
        if beige_book["text"] == []:
            beige_book["text"] = response.css("p *::text").getall()
        yield beige_book
