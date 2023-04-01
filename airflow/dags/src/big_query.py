from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq


def setup_client(key_path: str) -> bigquery.Client:
    #Get Credentials and authenticate
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def load_dataframe_to_bigquery(
        client: bigquery.Client,
        table_id: str, 
        df: pd.DataFrame
    ):
    job = client.load_table_from_dataframe(
        df, table_id
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}."
    )