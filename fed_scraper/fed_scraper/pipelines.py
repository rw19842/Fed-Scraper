# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter
from fed_scraper.items import (
    serialize_url,
    serialize_date,
    serialize_document_kind,
)
import re
import os.path
from os import mkdir
import pandas as pd
from datetime import datetime, timedelta
import logging

DATA_DIR = "../data/"
ALL_DOCS_FILE = "fomc_documents.csv"
ALL_DOCS_FILE_PATH = DATA_DIR + ALL_DOCS_FILE
SUB_DATA_DIR = DATA_DIR + "documents_by_type/"
MEETING_DATES_FILE_PATH = "../meeting_dates.csv"


class DuplicateUrlPipeline:
    def open_spider(self, spider):
        if os.path.isfile(ALL_DOCS_FILE_PATH):
            self.scraped_urls = set(pd.read_csv(ALL_DOCS_FILE_PATH)["url"])
        else:
            self.scraped_urls = []

    def process_item(self, item, spider):
        url = serialize_url(item.get("url"))
        if url in self.scraped_urls:
            raise DropItem(f"Item with url: {url} already in {ALL_DOCS_FILE}")
        return item


class TextPipeline:
    def process_item(self, item, spider):
        clean_text = []
        for text_part in item.get("text"):
            clean_text_part = re.sub(r"\x02", "", text_part)
            clean_text_part = re.sub(r"(\r\n)(\t)*(\r\n)", ".", text_part)
            clean_text_part = re.sub(r"[\n\r\t]", " ", text_part)
            if text_part != "":
                clean_text.append(clean_text_part)
        clean_text = " ".join(clean_text).strip()
        clean_text = re.sub(r" +", " ", clean_text)
        clean_text = re.sub(r" \.", ".", clean_text)
        clean_text = re.sub(r"\x02", "", clean_text)
        item["text"] = clean_text
        return item


class ReleaseDatesPipeline:
    def __init__(self):
        self.num_release_dates_filled = 0

        if os.path.isfile(MEETING_DATES_FILE_PATH):
            self.meeting_dates = [
                datetime.strptime(date_string, "%Y-%m-%d").date()
                for date_string in pd.read_csv(MEETING_DATES_FILE_PATH)["meeting_date"]
            ]
        else:
            self.meeting_dates = []

    def close_spider(self, spider):
        if self.num_release_dates_filled > 0:
            logging.warning(
                f"Inferred {self.num_release_dates_filled} missing release dates."
            )

    def process_item(self, item, spider):
        if item.get("release_date") is not None:
            return item
        else:
            self.num_release_dates_filled += 1

        document_kind = serialize_document_kind(item["document_kind"])
        meeting_date = serialize_date(item["meeting_date"])
        subsequent_meeting_date = self.get_subsequent_meeting_date(meeting_date)
        annual_report_date = datetime(meeting_date.year, 4, 1).date()

        if document_kind == "minutes":
            if meeting_date >= datetime(2004, 12, 1).date():
                item["release_date"] = meeting_date + timedelta(weeks=3)
            elif meeting_date >= datetime(1993, 2, 1).date():
                item["release_date"] = subsequent_meeting_date + timedelta(days=3)

        elif document_kind in ["record_of_policy_actions", "minutes_of_actions"]:
            if meeting_date >= datetime(1976, 1, 1).date():
                item["release_date"] = meeting_date + timedelta(days=30)
            elif meeting_date >= datetime(1975, 1, 1).date():
                item["release_date"] = meeting_date + timedelta(days=45)
            elif meeting_date >= datetime(1967, 1, 1).date():
                item["release_date"] = meeting_date + timedelta(days=90)
            else:
                item["release_date"] = annual_report_date

        elif document_kind in [
            "historical_minutes",
            "intermeeting_executive_committee_minutes",
        ]:
            item["release_date"] = max(
                datetime(1964, 1, 1).date(), meeting_date + timedelta(days=5 * 365.25)
            )

        elif document_kind == "transcript":
            item["release_date"] = max(
                datetime(1993, 11, 1).date(), meeting_date + timedelta(days=5 * 365.25)
            )

        elif document_kind in [
            "press_conference",
            "statement",
            "implementation_note",
        ]:
            item["release_date"] = meeting_date

        elif document_kind in [
            "memoranda_of_discussion",
            "agenda",
            "greenbook",
            "greenbook_part_one",
            "greenbook_part_two",
            "greenbook_supplement",
            "tealbook_a",
            "tealbook_b",
        ]:
            item["release_date"] = meeting_date + timedelta(days=5 * 365.25)

        else:
            self.num_release_dates_filled -= 1

        return item

    def get_subsequent_meeting_date(self, date_input):
        if date_input not in self.meeting_dates:
            logging.warning(
                f"Meeting dates file may be incomplete:"
                f" {date_input} is not in {MEETING_DATES_FILE_PATH}"
            )

        subsequent_date = datetime.max.date()
        for date in self.meeting_dates:
            if date <= date_input:
                pass
            else:
                if date < subsequent_date:
                    subsequent_date = date

        if subsequent_date == datetime.max.date():
            logging.warning(
                f"Assuming subsequent meeting to {date_input} is in 6 weeks."
            )
            subsequent_date = date_input + timedelta(weeks=6)

        return subsequent_date


class MeetingDatesPipeline:
    def __init__(self):
        if os.path.isfile(MEETING_DATES_FILE_PATH):
            self.meeting_dates = [
                datetime.strptime(date_string, "%Y-%m-%d").date()
                for date_string in pd.read_csv(MEETING_DATES_FILE_PATH)["meeting_date"]
            ]
        else:
            self.meeting_dates = None

    def process_item(self, item, spider):
        if self.meeting_dates is None:
            logging.warning(f"MeetingDatesPipeline requires {MEETING_DATES_FILE_PATH}")
            return item

        meeting_date = datetime.max.date()
        item["release_date"] = serialize_date(item.get("release_date"))
        release_date = item.get("release_date")
        for date in self.meeting_dates:
            if date > release_date and date < meeting_date:
                meeting_date = date

        if meeting_date == datetime.max.date():
            logging.warning(
                f"Either meeting has not happened yet or {MEETING_DATES_FILE_PATH} is incomplete."
            )
        else:
            item["meeting_date"] = meeting_date

        return item


class RemoveMissingPipeline:
    def __init__(self):
        self.check_missing = ["document_kind", "text", "url"]
        self.num_missing = 0

    def process_item(self, item, spider):
        for field in self.check_missing:
            if field not in item:
                self.num_missing += 1
                raise DropItem(f"Item missing {field}")

        if bool(re.fullmatch(r"\s*", item.get("text"))):
            self.num_missing += 1
            raise DropItem("Item text field is whitespace")

        return item

    def close_spider(self, spider):
        if self.num_missing > 0:
            logging.warning(
                f"Removed {self.num_missing} documents with missing values:"
            )


class CsvPipeline:
    def open_spider(self, spider):
        if os.path.isfile(ALL_DOCS_FILE_PATH):
            include_headers_line = False
        else:
            include_headers_line = True
            if not os.path.isdir(DATA_DIR):
                mkdir(DATA_DIR)

        self.file = open(ALL_DOCS_FILE_PATH, "ab")
        self.exporter = CsvItemExporter(
            file=self.file,
            include_headers_line=include_headers_line,
        )
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class PostExportPipeline:
    def process_item(self, item, spider):
        return item


class DuplicatesPipeline(PostExportPipeline):
    def close_spider(self, spider):
        all_fomc_documents = pd.read_csv(ALL_DOCS_FILE_PATH)

        duplicated = all_fomc_documents.duplicated(subset="url", keep="last")

        if duplicated.any():
            for index, row in all_fomc_documents[duplicated].iterrows():
                logging.info(
                    "Removed duplicate:"
                    f"document_kind: {row['document_kind']}, "
                    f"meeting_date: {row['meeting_date']}, "
                    f"release_date: {row['release_date']}, "
                    f"url:{row['url']}, "
                )
            logging.warning(
                f"Removing {duplicated.sum()} duplicate document(s) found in {ALL_DOCS_FILE}:"
            )

        all_fomc_documents.drop_duplicates(subset="url", keep="last", inplace=True)

        all_fomc_documents.to_csv(
            ALL_DOCS_FILE_PATH,
            index=False,
        )


class SortByMeetingDatePipeline(PostExportPipeline):
    def close_spider(self, spider):
        all_fomc_documents = pd.read_csv(ALL_DOCS_FILE_PATH)
        all_fomc_documents.sort_values(
            by="meeting_date",
            inplace=True,
            na_position="first",
        )

        all_fomc_documents.to_csv(
            ALL_DOCS_FILE_PATH,
            index=False,
        )


class SplitCsvPipeline(PostExportPipeline):
    def close_spider(self, spider):
        if not os.path.isdir(SUB_DATA_DIR):
            mkdir(SUB_DATA_DIR)

        files = [
            {
                "name": "meeting_transcripts.csv",
                "document_kinds": ["transcript"],
            },
            {
                "name": "meeting_minutes.csv",
                "document_kinds": [
                    "minutes",
                    "minutes_of_actions",
                    "record_of_policy_actions",
                    "memoranda_of_discussion",
                    "historical_minutes",
                    "intermeeting_executive_committee_minutes",
                ],
            },
            {
                "name": "press_conference_transcript.csv",
                "document_kinds": ["press_conference"],
            },
            {
                "name": "policy_statements.csv",
                "document_kinds": ["statement", "implementation_note"],
            },
            {
                "name": "agendas.csv",
                "document_kinds": ["agenda"],
            },
            {
                "name": "greenbooks.csv",
                "document_kinds": [
                    "greenbook",
                    "greenbook_part_one",
                    "greenbook_part_two",
                    "greenbook_supplement",
                    "tealbook_a",
                ],
            },
            {
                "name": "bluebooks.csv",
                "document_kinds": ["bluebook", "tealbook_b"],
            },
            {
                "name": "redbooks.csv",
                "document_kinds": ["redbook", "beige_book"],
            },
        ]

        all_fomc_documents = pd.read_csv(ALL_DOCS_FILE_PATH)

        non_misc_document_kinds = []
        for file in files:
            non_misc_document_kinds += file["document_kinds"]
        misc_document_kinds = [
            document_kind
            for document_kind in set(all_fomc_documents["document_kind"])
            if document_kind not in non_misc_document_kinds
        ]
        files.append(
            {"name": "miscellaneous.csv", "document_kinds": misc_document_kinds}
        )

        for file in files:
            df = all_fomc_documents[
                all_fomc_documents["document_kind"].isin(file["document_kinds"])
            ].copy()

            df.sort_values(by="meeting_date", inplace=True, na_position="first")
            df.drop_duplicates(subset="url", keep="last", inplace=True)
            df.to_csv(
                SUB_DATA_DIR + file["name"],
                index=False,
            )
