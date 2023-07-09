import scrapy


class FomcCalendarSpider(scrapy.Spider):
    name = "fomc_calendar"
    allowed_domains = ["www.federalreserve.gov"]
    start_urls = ["https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"]

    def parse(self, response):
        years = response.css(".panel-default")
        for year in years:
            meetings = year.css(".fomc-meeting")
            for meeting in meetings:
                meeting_urls = dict()
                for _ in meeting.css("div div a"):
                    if _.css("::text").get() == "HTML":
                        if "minutes" in _.css("::attr(href)").get():
                            meeting_urls["Minutes url"] = (
                                "https://www.federalreserve.gov"
                                + _.css("::attr(href)").get()
                            )
                        else:
                            meeting_urls["Statement url"] = (
                                "https://www.federalreserve.gov"
                                + _.css("::attr(href)").get()
                            )
                    elif _.css("::text").get() != "PDF":
                        meeting_urls[_.css("::text").get() + " url"] = (
                            "https://www.federalreserve.gov"
                            + _.css("::attr(href)").get()
                        )
                yield meeting_urls
