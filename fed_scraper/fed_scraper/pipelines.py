# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
from fed_scraper.items import DOCUMENT_KINDS
import re


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
    def open_spider(self, spider):
        self.files = dict(
            [(name, open("../data/" + name + ".csv", "ab")) for name in DOCUMENT_KINDS]
        )

        self.exporters = dict(
            [(name, CsvItemExporter(self.files[name])) for name in DOCUMENT_KINDS]
        )

        [e.start_exporting() for e in self.exporters.values()]

    def close_spider(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]

    def process_item(self, item, spider):
        self.exporters[item["document_kind"]].export_item(item)
        return item
