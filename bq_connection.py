from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()


class BQConnection:
    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_file(
            os.getenv("BQ_FILE_PATH"),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

    def execute(self, query):
        self.client.query(query).result()

    def fetch(self, query):
        return self.client.query(query).to_dataframe()

    def load_data(self, dataframe, table_id):
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        self.client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        ).result()
