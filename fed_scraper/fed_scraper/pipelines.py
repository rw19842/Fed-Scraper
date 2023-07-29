# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
from fed_scraper.items import FedScraperItem
import re
import os.path
import pandas as pd


class FedScraperPipeline:
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


class MultiCsvItemPipeline:
    data_directory = "../data/"
    all_docs_file = "fomc_documents.csv"
    file_path = data_directory + all_docs_file

    def open_spider(self, spider):
        if os.path.isfile(self.file_path):
            include_headers_line = False
        else:
            include_headers_line = True

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

        self.delete_duplicates()
        self.fill_release_dates()
        self.split_csv_by_doc_type()

    def process_item(self, item, spider):
        self.exporter.export_item(item)

    def delete_duplicates(self):
        pass

    def fill_release_dates(self):
        pass

    def split_csv_by_doc_type(self):
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
