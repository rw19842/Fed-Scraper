import scrapy
import re
from fed_scraper.items import FedScraperItem, serialize_url
from fed_scraper.parse_pdf import parse_pdf_from_url


class FomcCalendarSpider(scrapy.Spider):
    name = "fomc_calendar"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = ["https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"]

    def parse(self, response):
        year_panels = response.css(".panel-default")
        for year_panel in year_panels:
            meeting_year = year_panel.css(".panel-heading *::text").get()

            meeting_panels = year_panel.css(".fomc-meeting")
            for meeting_panel in meeting_panels:
                meeting_month = meeting_panel.css(".fomc-meeting__month *::text").get()
                meeting_date = meeting_panel.css(".fomc-meeting__date *::text").get()
                meeting_date_str = " ".join([meeting_month, meeting_date, meeting_year])

                minutes_panel = meeting_panel.css(".fomc-meeting__minutes")
                if "HTML" in minutes_panel.css("a::text").getall():
                    minutes = FedScraperItem(
                        document_kind="minutes",
                        meeting_date=meeting_date_str,
                    )

                    minutes["url"] = minutes_panel.css("a::attr(href)").getall()[
                        minutes_panel.css("a::text").getall().index("HTML")
                    ]

                    minutes["release_date"] = re.search(
                        r"(?<=\(Released )(.*?)(?=\))",
                        "".join(minutes_panel.css("*::text").getall()),
                    ).group()

                    yield response.follow(
                        minutes["url"],
                        callback=self.parse_minutes,
                        cb_kwargs={"minutes": minutes},
                    )

                statement_panel = meeting_panel.css(".col-lg-2")
                for anchor in statement_panel.css("a"):
                    if anchor.css("::text").get() == "HTML":
                        statement = FedScraperItem(
                            document_kind="statement",
                            meeting_date=meeting_date_str,
                            url=anchor.css("::attr(href)").get(),
                        )
                        yield response.follow(
                            statement["url"],
                            callback=self.parse_statement,
                            cb_kwargs={"statement": statement},
                        )

                    if anchor.css("::text").get() == "Implementation Note":
                        implementation_note = FedScraperItem(
                            document_kind="implementation_note",
                            meeting_date=meeting_date_str,
                            url=anchor.css("::attr(href)").get(),
                        )
                        yield response.follow(
                            implementation_note["url"],
                            callback=self.parse_implementation_note,
                            cb_kwargs={"implementation_note": implementation_note},
                        )

                press_conference_panel = meeting_panel.css(".col-md-4:nth-child(4)")
                for anchor in press_conference_panel.css("a"):
                    if bool(
                        re.search(
                            r"PRESS CONFERENCE",
                            anchor.css("::text").get(),
                            flags=re.I,
                        )
                    ):
                        press_conference = FedScraperItem(
                            document_kind="press_conference",
                            meeting_date=meeting_date_str,
                        )

                        press_conference_page_url = anchor.css("::attr(href)").get()

                        yield response.follow(
                            press_conference_page_url,
                            callback=self.parse_press_conference,
                            cb_kwargs={"press_conference": press_conference},
                        )

    def parse_minutes(self, response, minutes):
        minutes["text"] = response.css("#article *::text").getall()
        yield minutes

    def parse_statement(self, response, statement):
        statement["release_date"] = response.css(".article__time::text").get().strip()
        statement["text"] = response.css(".col-md-8")[1:].css("*::text").getall()
        yield statement

    def parse_press_conference(self, response, press_conference):
        for anchor in response.css(".panel-padded a"):
            if bool(
                re.search(
                    r"(PRESS CONFERENCE TRANSCRIPT)",
                    anchor.css("::text").get(),
                    flags=re.I,
                )
            ):
                press_conference["url"] = anchor.css("::attr(href)").get()
                break

        press_conference["text"] = parse_pdf_from_url(
            serialize_url(press_conference["url"])
        )

        yield press_conference

    def parse_implementation_note(self, response, implementation_note):
        implementation_note["release_date"] = (
            response.css(".article__time::text").get().strip()
        )

        implementation_note["text"] = response.css(
            ".row~ .row+ .row .col-xs-12 *::text"
        ).getall()

        yield implementation_note
