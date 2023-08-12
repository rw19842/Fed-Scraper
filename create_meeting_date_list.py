import pandas as pd

DATA_DIR = "data/"
ALL_DOCS_FILE = "fomc_documents.csv"
ALL_DOCS_FILE_PATH = DATA_DIR + ALL_DOCS_FILE

if __name__ == "__main__":
    dates = pd.read_csv(ALL_DOCS_FILE_PATH)["meeting_date"]
    dates.dropna(inplace=True)
    dates.drop_duplicates(inplace=True)
    dates = pd.to_datetime(dates)
    dates.sort_values(inplace=True)
    dates.to_csv("meeting_dates.csv", index=False)
