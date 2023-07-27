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
        self.split_csv_by_doc_kind()

    def process_item(self, item, spider):
        self.exporter.export_item(item)

    def delete_duplicates(self):
        pass

    def split_csv_by_doc_kind(self):
        fomc_documents = pd.read_csv(self.file_path)
        document_kinds = list(set(fomc_documents["document_kind"]))
        for document_kind in document_kinds:
            documents_df = fomc_documents[
                fomc_documents["document_kind"] == document_kind
            ].copy()
            documents_df.drop("document_kind", axis=1, inplace=True)
            documents_df.sort_values(
                by="meeting_date", inplace=True, na_position="first"
            )
            documents_df.to_csv(
                self.data_directory + "documents_by_kind/" + document_kind + ".csv",
                index=False,
            )
