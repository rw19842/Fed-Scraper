import scrapy
from fed_scraper.items import FedScraperItem, serialize_url
from fed_scraper.spiders.fomc_calendar import parse_pdf_from_url
import re


class HistoricalMaterialsSpider(scrapy.Spider):
    name = "historical_materials"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = [
        "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm"
    ]

    def parse(self, response):
        for anchor in response.css("div div .panel-padded a"):
            meeting_year = anchor.css("::text").get()
            meeting_year_page_url = anchor.css("::attr(href)").get()
            yield response.follow(
                meeting_year_page_url,
                callback=self.parse_year_page,
                cb_kwargs={"meeting_year": meeting_year},
            )

    def parse_year_page(self, response, meeting_year):
        meeting_panels = response.css(".panel-default")

        for meeting_panel in meeting_panels:
            meeting_date_str = meeting_panel.css("h5::text").get()
            for anchor in meeting_panel.css("a"):
                fed_scraper_item = FedScraperItem(
                    meeting_date=meeting_date_str,
                    url=anchor.css("::attr(href)").get(),
                )

                anchor_text = anchor.css("::text").get()
                if anchor_text.upper() in ["PDF", "HTML"]:
                    surrounding_text = anchor.xpath("..").css("::text").get()
                    fed_scraper_item["document_kind"] = surrounding_text
                    if bool(re.search(r"released", surrounding_text, re.I)):
                        fed_scraper_item["release_date"] = re.search(
                            r"released .*",
                            surrounding_text,
                            re.I,
                        ).group()
                else:
                    fed_scraper_item["document_kind"] = anchor_text

                    surrounding_text = " ".join(
                        anchor.xpath("..").css("::text").getall()
                    )
                    if bool(re.search(r"released", surrounding_text, re.I)):
                        fed_scraper_item["release_date"] = re.search(
                            r"released .*",
                            surrounding_text,
                            re.I,
                        ).group()

                if ".pdf" in fed_scraper_item["url"]:
                    fed_scraper_item["text"] = parse_pdf_from_url(
                        serialize_url(fed_scraper_item["url"])
                    )
                    yield fed_scraper_item
                else:
                    yield response.follow(
                        fed_scraper_item["url"],
                        callback=self.parse_html_document_page,
                        cb_kwargs={"fed_scraper_item": fed_scraper_item},
                    )

    def parse_html_document_page(self, response, fed_scraper_item):
        fed_scraper_item["text"] = response.css("#article *::text").getall()
        yield fed_scraper_item
