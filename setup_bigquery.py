from google.cloud import bigquery
from airflow.dags.src.utils import get_key_file_name
from airflow.dags.src.bigquery import setup_client

def create_dataset(client: bigquery.Client, dataset_name: str):
    dataset_id = f"{client.project}.{dataset_name}"
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    except Exception as e:
        print(e)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_tweets_table(client, dataset):
    table_id = f"{client.project}.{dataset}.Twitter"
    schema = [
        bigquery.SchemaField("tweet_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("retweet_count", "INTEGER"),
        bigquery.SchemaField("like_count", "INTEGER"),
        bigquery.SchemaField("quote_count", "INTEGER"),
        bigquery.SchemaField("time_pulled", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("sentiment", "INTEGER", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:  
        table = client.create_table(table)  # Make an API request.
    except Exception as e:
        print(e)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_reddit_table(client, dataset):
    table_id = f"{client.project}.{dataset}.Reddit"
    schema = [
        bigquery.SchemaField("stock_ticker", "STRING"),
        bigquery.SchemaField("subreddit", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("upvotes", "FLOAT"),
        bigquery.SchemaField("num_comments", "FLOAT"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("created_time", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:  
        table = client.create_table(table)  # Make an API request.
    except Exception as e:
        print(e)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_company_table(client, dataset):
    table_id = f"{client.project}.{dataset}.Companies"
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("market_cap", "INTEGER"),
        bigquery.SchemaField("shares_outstanding", "INTEGER"),
        bigquery.SchemaField("beta", "FLOAT"),
        bigquery.SchemaField("earnings_quarterly_growth", "FLOAT"),
        bigquery.SchemaField("earnings_annual_growth", "FLOAT"),
        bigquery.SchemaField("dividend_yield", "FLOAT"),
        bigquery.SchemaField("trailing_pe", "FLOAT"),
        bigquery.SchemaField("forward_pe", "FLOAT"),
        bigquery.SchemaField("trailing_eps", "FLOAT"),
        bigquery.SchemaField("forward_eps", "FLOAT"),
        bigquery.SchemaField("peg_ratio", "FLOAT"),
        bigquery.SchemaField("updated_time", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:  
        table = client.create_table(table)  # Make an API request.
    except Exception as e:
        print(e)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def setup_bigquery():
    key_file = get_key_file_name()
    client = setup_client(f'./key/{key_file}')
    create_dataset(client, 'Data')
    create_tweets_table(client, 'Data')
    create_reddit_table(client, 'Data')
    create_company_table(client, 'Data')


if __name__ == '__main__':
    setup_bigquery()