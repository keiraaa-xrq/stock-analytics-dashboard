#Import BigQuery and Google Authentication Packages
from google.cloud import bigquery
from google.oauth2 import service_account

# top 30 US tech firms by market cap
ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
"CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
"BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]

def initialise_client():
    #Get Credentials and authenticate
    key_path = "./token/is3107-grp18-e8944871c568.json" # use your own token
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def initialise_dataset(client):
    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.Yahoo".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def initialise_table(client):
    # create table schema
    schema=[
            bigquery.SchemaField("Datetime", "TIMESTAMP"),
            bigquery.SchemaField("Open", "FLOAT"),
            bigquery.SchemaField("High", "FLOAT"),
            bigquery.SchemaField("Low", "FLOAT"),
            bigquery.SchemaField("Close", "FLOAT"),
            bigquery.SchemaField("Adj_Close", "FLOAT"),
            bigquery.SchemaField("Volume", "INTEGER")
        ]
    project=client.project

    # initialise bigquery table for each stock
    for ticker in ticker_list:
        table_id = f'{project}.Yahoo.{ticker}'
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

if __name__ == "__main__":
    client = initialise_client()
    initialise_dataset(client)
    initialise_table(client)
