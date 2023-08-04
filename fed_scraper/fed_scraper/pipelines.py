# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
from fed_scraper.items import FedScraperItem, serialize_date, serialize_document_kind
import re
import os.path
from os import mkdir
import pandas as pd
from datetime import datetime, timedelta


class TextPipeline:
    def process_item(self, item, spider):
        text_list = item["text"]
        clean_text = []
        for text_part in text_list:
            clean_text_part = re.sub(r"[\n\r\t]", " ", text_part).strip()
            if text_part != "":
                clean_text.append(clean_text_part)
        clean_text = " ".join(clean_text).strip()
        clean_text = re.sub(r" +", " ", clean_text)
        clean_text = re.sub(r" \.", ".", clean_text)
        item["text"] = clean_text
        return item


class ReleaseDatesPipeline:
    def process_item(self, item, spider):
        if item.get("release_date") is not None:
            return item

        document_kind = serialize_document_kind(item["document_kind"])
        meeting_date = serialize_date(item["meeting_date"])
        subsequent_meeting_date = meeting_date + timedelta(weeks=6)
        annual_report_date = datetime(meeting_date.year, 4, 1)

        if document_kind == "minutes":
            if meeting_date >= datetime(2004, 12, 1):
                item["release_date"] = meeting_date + timedelta(weeks=3)
            elif meeting_date >= datetime(1993, 2, 1):
                item["release_date"] = subsequent_meeting_date + timedelta(days=3)

        elif document_kind in ["record_of_policy_actions", "minutes_of_actions"]:
            if meeting_date >= datetime(1976, 1, 1):
                item["release_date"] = meeting_date + timedelta(days=30)
            elif meeting_date >= datetime(1975, 1, 1):
                item["release_date"] = meeting_date + timedelta(days=45)
            elif meeting_date >= datetime(1967, 1, 1):
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

        elif document_kind == "memoranda_of_discussion":
            item["release_date"] = meeting_date + timedelta(days=5 * 365.25)

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

        else:
            item["release_date"] = meeting_date + timedelta(days=5 * 365.25)

        return item


class CsvPipeline:
    data_directory = "../data/"
    all_docs_file = "fomc_documents.csv"
    file_path = data_directory + all_docs_file

    def open_spider(self, spider):
        if os.path.isfile(self.file_path):
            include_headers_line = False
        else:
            include_headers_line = True
            if not os.path.isdir(self.data_directory):
                mkdir(self.data_directory)

        self.file = open(self.file_path, "ab")
        self.exporter = CsvItemExporter(
            file=self.file,
            include_headers_line=include_headers_line,
            fields_to_export=list(FedScraperItem.fields),
        )
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class PostExportPipeline(CsvPipeline):
    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        pass


class DuplicatesPipeline(PostExportPipeline):
    def close_spider(self, spider):
        pass


class SortByMeetingDatePipeline(PostExportPipeline):
    def close_spider(self, spider):
        all_fomc_documents = pd.read_csv(self.file_path)
        all_fomc_documents.sort_values(
            by="meeting_date",
            inplace=True,
            na_position="first",
        )
        all_fomc_documents.to_csv(
            self.file_path,
            index=False,
        )


class SplitCsvPipeline(PostExportPipeline):
    def close_spider(self, spider):
        if not os.path.isdir(self.data_directory + "documents_by_type/"):
            mkdir(self.data_directory + "documents_by_type/")

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
            {"name": "agendas.csv", "document_kinds": ["agenda"]},
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

        all_fomc_documents = pd.read_csv(self.file_path)

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
            df.to_csv(
                self.data_directory + "documents_by_type/" + file["name"],
                index=False,
            )
