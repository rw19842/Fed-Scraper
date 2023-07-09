import scrapy
import re


class FomcCalendarSpider(scrapy.Spider):
    name = "fomc_calendar"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = ["https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"]

    def parse(self, response):
        year_panels = response.css(".panel-default")
        for year_panel in year_panels:
            meeting_year = year_panel.css("div div *::text").get()[:4]
            meeting_panels = year_panel.css(".fomc-meeting")
            for meeting_panel in meeting_panels:
                meeting_month = meeting_panel.css(".fomc-meeting__month *::text").get()
                meeting_date = meeting_panel.css(".fomc-meeting__date *::text").get()

                minutes_panel = meeting_panel.css(".fomc-meeting__minutes")
                if "HTML" in minutes_panel.css("a::text").getall():
                    minutes_relative_url = minutes_panel.css("a::attr(href)").getall()[
                        minutes_panel.css("a::text").getall().index("HTML")
                    ]
                    meeting_minutes_url = (
                        "https://www.federalreserve.gov" + minutes_relative_url
                    )
                    yield response.follow(
                        meeting_minutes_url, callback=self.parse_minutes
                    )

                    minutes_release_date = re.search(
                        r"(?<=\(Released )(.*?)(?=\))",
                        "".join(minutes_panel.css("*::text").getall()),
                    ).group()

                statement_panel = meeting_panel.css(".col-lg-2")
                for anchor in statement_panel.css("a"):
                    if anchor.css("::text").get() == "HTML":
                        statement_relative_url = anchor.css("::attr(href)").get()
                        statement_url = (
                            "https://www.federalreserve.gov" + statement_relative_url
                        )
                        yield response.follow(
                            statement_url, callback=self.parse_statement
                        )

                    if anchor.css("::text").get() == "Implementation Note":
                        implementation_note_relative_url = anchor.css(
                            "::attr(href)"
                        ).get()
                        implementation_note_url = (
                            "https://www.federalreserve.gov"
                            + implementation_note_relative_url
                        )
                        yield response.follow(
                            implementation_note_url,
                            callback=self.parse_implementation_note,
                        )

                press_conference_panel = meeting_panel.css(".col-lg-3")
                for anchor in press_conference_panel.css("a"):
                    if anchor.css("::text").get() == "Press Conference":
                        press_conference_relative_url = anchor.css("::attr(href)").get()
                        press_conference_url = (
                            "https://www.federalreserve.gov"
                            + press_conference_relative_url
                        )
                        yield response.follow(
                            press_conference_url, callback=self.parse_press_conference
                        )

    def parse_minutes(self, response):
        pass

    def parse_statement(self, response):
        pass

    def parse_press_conference(self, response):
        pass

    def parse_implementation_note(self, response):
        pass

    def parse_projections(self, response):
        pass

    def parse_long_run_goals_and_MP_strategy(self, response):
        # maybe easier to write a spider for:
        # https://www.federalreserve.gov/monetarypolicy/historical-statements-on-longer-run-goals-and-monetary-policy-strategy.htm
        pass
