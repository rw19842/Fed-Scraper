import scrapy
import re
from fed_scraper.items import FedScraperItem, serialize_url
from fed_scraper.parse_pdf import parse_pdf_from_url


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
                surrounding_text = " ".join(anchor.xpath("..").css("::text").getall())

                if bool(
                    re.fullmatch(
                        r"((\d*\.?\d*) ((MB)|(KB)) (PDF))|(PDF)|(HTML)",
                        re.sub(r"\(.*\)", "", anchor_text),
                    )
                ):
                    fed_scraper_item["document_kind"] = surrounding_text
                elif "GREENBOOK" in surrounding_text.upper():
                    if "PART 1" in anchor_text.upper():
                        fed_scraper_item["document_kind"] = "greenbook_part_one"
                    elif "PART 2" in anchor_text.upper():
                        fed_scraper_item["document_kind"] = "greenbook_part_two"
                    elif "SUPPLEMENT" in anchor_text.upper():
                        fed_scraper_item["document_kind"] = "greenbook_supplement"
                    else:
                        fed_scraper_item["document_kind"] = anchor_text
                elif "SUPPLEMENT" in anchor_text.upper():
                    fed_scraper_item["document_kind"] = "greenbook_supplement"
                elif "BEIGE" in surrounding_text.upper():
                    fed_scraper_item["document_kind"] = "beige_book"
                elif (
                    "INTERMEETING EXECUTIVE COMMITTEE MINUTES"
                    in surrounding_text.upper()
                ):
                    fed_scraper_item[
                        "document_kind"
                    ] = "intermeeting_executive_committee_minutes"
                    fed_scraper_item["meeting_date"] = anchor_text
                else:
                    fed_scraper_item["document_kind"] = anchor_text

                if "RELEASED" in surrounding_text.upper():
                    fed_scraper_item["release_date"] = re.search(
                        r"(?<=\()(Released.*?)(?=\))",
                        surrounding_text,
                        re.I,
                    ).group()

                if anchor_text == "HTML" and bool(
                    re.search(
                        r"((\d*\.?\d* (KB|MB))? PDF\s*\|\s*HTML)|(HTML\s*\|\s*(\d*\.?\d* (KB|MB))? PDF)",
                        surrounding_text,
                    )
                ):  # handling the case where there is both a PDF and HTML version of the same document
                    pass
                elif ".pdf" in fed_scraper_item["url"]:
                    fed_scraper_item["text"] = parse_pdf_from_url(
                        serialize_url(fed_scraper_item["url"])
                    )
                    yield fed_scraper_item
                elif ".htm" in fed_scraper_item["url"]:
                    yield response.follow(
                        fed_scraper_item["url"],
                        callback=self.parse_html_document_page,
                        cb_kwargs={"fed_scraper_item": fed_scraper_item},
                    )

    def parse_html_document_page(self, response, fed_scraper_item):
        fed_scraper_item["text"] = response.css("#article *::text").getall()
        if fed_scraper_item["text"] == []:
            fed_scraper_item["text"] = response.css("p *::text").getall()
        yield fed_scraper_item
