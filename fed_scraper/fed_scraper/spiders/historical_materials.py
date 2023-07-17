import scrapy


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
        if response.css(".panel-padded") != []:
            meeting_panels = response.css(".panel-padded")
        elif response.css(".panel-default") != []:
            meeting_panels = response.css(".panel-default")
        else:
            raise Exception(f"No 'meeting_panels' found for year {meeting_year}")

        for meeting_panel in meeting_panels:
            date_string = meeting_panel.css(".panel-heading h5::text").get()
            with open("/Users/edwardbickerton/Desktop/hist_meetings.txt", "a") as file:
                file.write(date_string)
                file.write("\n")
                file.close()
