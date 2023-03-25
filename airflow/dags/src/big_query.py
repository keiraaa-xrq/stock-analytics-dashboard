from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq

key_path = "key/is3107-grp18-e8944871c568.json" # use your own token
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = 'is3107-grp18'

def load_dataframe_to_bigquery(df: pd.DataFrame, table_id: str):
    pandas_gbq.to_gbq(df, table_id, if_exists='append')