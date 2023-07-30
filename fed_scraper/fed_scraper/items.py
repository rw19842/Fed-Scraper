# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime
import calendar
import re


def serialize_document_kind(kind: str):
    kind = re.sub(r"\(.*\)", "", kind)
    kind = re.sub(r"(a\.m)|(p\.m)|(PDF)|(HTML)|(MB)|(KB)", " ", kind)
    kind = re.sub(r"\d", " ", kind)
    kind = re.sub(r"[\._,-:\|]", " ", kind)
    kind = kind.lower()
    kind = kind.strip()
    kind = re.sub(r"\s+", "_", kind)
    return kind


def serialize_date(date_string: str):
    formats = [
        "%d %B %Y",  # 02 February 2023
        "%d %b %Y",  # 02 Feb 2023
    ]

    for date_format in formats:
        try:
            return datetime.strptime(date_string, date_format).date()
        except ValueError:
            pass

    date_string = re.sub(r"\(.*\)", "", date_string)
    for part in re.sub(r"\W", " ", date_string).split():
        if part in calendar.month_name[1:] or part in calendar.month_abbr[1:]:
            month = part
        if bool(re.fullmatch(r"\d\d?", part)):
            date = part
        if bool(re.fullmatch(r"\d\d\d\d", part)):
            year = part
    return serialize_date(f"{date} {month} {year}")


def serialize_url(relative_url: str):
    absolute_url = "https://www.federalreserve.gov" + relative_url
    return absolute_url


class FedScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    document_kind = scrapy.Field(serializer=serialize_document_kind)
    release_date = scrapy.Field(serializer=serialize_date)
    meeting_date = scrapy.Field(serializer=serialize_date)
    url = scrapy.Field(serializer=serialize_url)
    text = scrapy.Field()
