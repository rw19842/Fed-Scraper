import pandas as pd
import re
from os import mkdir, listdir
import os.path


DATA_DIR = "data/"
SUB_DATA_DIR = DATA_DIR + "documents_by_type/"
ALL_DOCS_FILE = "fomc_documents.csv"
ALL_DOCS_FILE_PATH = DATA_DIR + ALL_DOCS_FILE
CSV_DESC_DIR = "csv_descriptions/"


def csv_file_stats(file_path):
    stats = {
        "Document Kind": [],
        "Count": [],
        "Earliest": [],
        "Latest": [],
    }

    documents_df = pd.read_csv(file_path)
    for document_kind in set(documents_df["document_kind"]):
        df = documents_df[documents_df["document_kind"] == document_kind]

        stats["Document Kind"].append(document_kind)
        stats["Count"].append(len(df))
        stats["Earliest"].append(df["meeting_date"].min())
        stats["Latest"].append(df["meeting_date"].max())

    stats_df = pd.DataFrame(stats).sort_values(by="Count", ascending=False)

    return stats_df, documents_df


def stats_df_to_md(stats_df, filename):
    stats_df.to_markdown(
        CSV_DESC_DIR + re.sub(r".csv", "", filename) + ".md",
        index=False,
    )


if __name__ == "__main__":
    if not os.path.isdir(CSV_DESC_DIR):
        mkdir(CSV_DESC_DIR)

    csv_overview = {
        "File": [],
        "Count": [],
        "Earliest": [],
        "Latest": [],
    }

    stats_df, documents_df = csv_file_stats(ALL_DOCS_FILE_PATH)
    stats_df_to_md(stats_df, ALL_DOCS_FILE)
    csv_overview["File"].append(
        f"[**`{ALL_DOCS_FILE}`**]({re.sub('.csv', '.md', ALL_DOCS_FILE)})"
    )
    csv_overview["Count"].append(len(documents_df))
    csv_overview["Earliest"].append(documents_df["meeting_date"].min())
    csv_overview["Latest"].append(documents_df["meeting_date"].max())

    for filename in listdir(SUB_DATA_DIR):
        stats_df, documents_df = csv_file_stats(SUB_DATA_DIR + filename)
        stats_df_to_md(stats_df, filename)

        csv_overview["File"].append(
            f"[**`{filename}`**]({re.sub('.csv', '.md', filename)})"
        )
        csv_overview["Count"].append(len(documents_df))
        csv_overview["Earliest"].append(documents_df["meeting_date"].min())
        csv_overview["Latest"].append(documents_df["meeting_date"].max())

    stats_df_to_md(
        pd.DataFrame(csv_overview).sort_values(by="Count", ascending=False),
        "csv_overview",
    )
