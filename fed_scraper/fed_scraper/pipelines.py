# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter


class FedScraperPipeline:
    def process_item(self, item, spider):
        return item


class MultiCsvItemPipeline:
    save_items = [
        "MeetingMinutes",
        "Statement",
        "PressConference",
        "ImplementationNote",
    ]

    def open_spider(self, spider):
        self.files = dict(
            [
                (name, open("../data/" + name + ".csv", "w+b"))
                for name in self.save_items
            ]
        )

        self.exporters = dict(
            [(name, CsvItemExporter(self.files[name])) for name in self.save_items]
        )

        [e.start_exporting() for e in self.exporters.values()]

    def close_spider(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]

    def process_item(self, item, spider):
        item_name = type(item).__name__
        if item_name in set(self.save_items):
            self.exporters[item_name].export_item(item)
        return item
