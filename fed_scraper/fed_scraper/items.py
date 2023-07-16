# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime


def serialize_date(date_string):
    formats = [
        "%B %d, %Y",  # February 02, 2023
        "%d %B %Y",  # 02 February 2023
        "%d %b %Y",  # 02 Feb 2023
    ]

    for date_format in formats:
        try:
            date_obj = datetime.strptime(date_string, date_format).date()
            return date_obj
        except ValueError:
            pass

    return None


def serialize_url(relative_url):
    absolute_url = "https://www.federalreserve.gov" + relative_url
    return absolute_url


class FedScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    release_date = scrapy.Field(serializer=serialize_date)
    meeting_date = scrapy.Field(serializer=serialize_date)


class MeetingDocument(FedScraperItem):
    url = scrapy.Field(serializer=serialize_url)


class Statement(MeetingDocument):
    text = scrapy.Field()


class ImplementationNote(MeetingDocument):
    text = scrapy.Field()


class PressConference(MeetingDocument):
    text = scrapy.Field()


class MeetingMinutes(MeetingDocument):
    text = scrapy.Field()
