from google.cloud import bigquery
from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass
class Config:
    project_id: str
    dataset_id: str
    table_id: str
    query: str


def main(args: Config):
    client = bigquery.Client(args.project_id)
    query_job = client.query(args.query)
    rows = query_job.result()
    df = rows.to_dataframe()
    return df


if __name__ == "__main__":
    args = Config(
        project_id=os.getenv("BQ_PROJECT_ID"),
        dataset_id=os.getenv("BQ_DATASET_ID"),
        table_id=os.getenv("BQ_TABLE_ID"),
    )
    query_test = f"""
        SELECT * FROM `{args.project_id}.{args.dataset_id}.{args.table_id}`
    """
    args.query = query_test
    df = main(args)
