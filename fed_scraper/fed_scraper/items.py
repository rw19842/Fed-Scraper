# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime

DOCUMENT_KINDS = {
    "meeting_minutes",
    "statement",
    "press_conference",
    "implementation_note",
}


def check_document_kind(kind: str):
    if kind in DOCUMENT_KINDS:
        return kind
    raise ValueError(f"INVALID DOCUMENT KIND: {kind}")


def serialize_date(date_string: str):
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


def serialize_url(relative_url: str):
    absolute_url = "https://www.federalreserve.gov" + relative_url
    return absolute_url


class FedScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    document_kind = scrapy.Field(serializer=check_document_kind)
    release_date = scrapy.Field(serializer=serialize_date)
    meeting_date = scrapy.Field(serializer=serialize_date)
    url = scrapy.Field(serializer=serialize_url)
    text = scrapy.Field()
