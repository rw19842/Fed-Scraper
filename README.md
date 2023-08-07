# Fed Scraper

This web scraper, built using the [Scrapy](https://scrapy.org/) framework, collects text data from various documents surrounding Federal Open Market Committee (FOMC) meetings found on the [Federal Reserve website](https://www.federalreserve.gov/).

## Spiders

This scrapy project consists of the following spiders:

1. [`fomc_calendar.py`]([/Fed-Scraper/fed_scraper/fed_scraper/spiders/fomc_calendar.py](https://github.com/rw19842/Fed-Scraper/blob/main/fed_scraper/fed_scraper/spiders/fomc_calendar.py))
   - Scrapes text from recent documents starting at: <https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm>
   - Takes $\approx$ 20 seconds to complete crawl
2. [`historical_materials.py`]([/fed_scraper/fed_scraper/spiders/historical_materials.py](https://github.com/rw19842/Fed-Scraper/blob/main/fed_scraper/fed_scraper/spiders/historical_materials.py))
   - Scrapes text from documents five or more years old starting at: <https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm>
   - Takes $\approx$ 45 minutes to complete crawl

## Usage

The spiders can be run with the [scrapy command line tool](https://docs.scrapy.org/en/latest/topics/commands.html) by running the [scrapy crawl command](https://docs.scrapy.org/en/latest/topics/commands.html#crawl) from the [scrapy project directory](fed_scraper).

Alternatively, the data will be made available on [kaggle](https://www.kaggle.com) at <https://www.kaggle.com/datasets/edwardbickerton/fomc-text-data>.

## Output

The scrapy spiders save each document into a row of the csv file, [`data/fomc_documents.csv`](data/fomc_documents.csv), which has the following columns:

1. `document_kind`
   - A list of document kinds in the dataset can be found [here](csv_descriptions/fomc_documents.md).
2. `meeting_date`
   - The date of the documents associated FOMC meeting.
3. `release_date`
   - The release date of the document.
   - When this is not found on the [Federal Reserve website](https://www.federalreserve.gov/) by the spider, it is inferred according the release schedules described [here](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm) via the scrapy pipeline [`ReleaseDatesPipeline`](fed_scraper/fed_scraper/pipelines.py).
4. `url`

The documents are then grouped based on their `document_kind` and split up into different csv files found in the [`data/documents_by_type`](data/documents_by_type) directory.

Details of the csv files can be found in [this table](csv_descriptions/csv_overview.md).
