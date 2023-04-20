from typing import *
import os
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from src.company_info import get_companies_info
from src.utils import get_bigquery_key
from src.bigquery import setup_client, load_dataframe_to_bigquery


@dag(
    description='Company Data Pipeline',
    schedule='@monthly',
    start_date=pendulum.datetime(2023, 1, 1, 0, 0, 0),
    catchup=False
)
def company_dag():

    @task
    def extract_company_task() -> List[Dict]:
        """
        Extract company info.
        """
        os.environ['NO_PROXY'] = "URL"
        company_list = get_companies_info()
        return company_list
    
    @task
    def transform_company_task(
        company_list: List[Dict],
        data_interval_end = None, 
    ) -> List[Dict]:
        """
        Add time updated.
        """
        os.environ['NO_PROXY'] = "URL"
        dte = data_interval_end
        updated_time = pendulum.datetime(dte.year, dte.month, dte.day).int_timestamp
        if len(company_list) > 0:
            for dic in company_list:
                dic['updated_time'] = updated_time
            return company_list
        else:
            return []

    @task
    def load_company_task(company_list: List[Dict]):
        """
        Load to bigquery.
        """
        os.environ['NO_PROXY'] = "URL"
        if len(company_list) > 0:
            # generate df
            company_df = pd.DataFrame(company_list)
            company_df['updated_time'] = company_df['updated_time'].apply(lambda x: pendulum.from_timestamp(x))
            # set up bigquery client
            key_file = get_bigquery_key()
            client = setup_client(f'./key/{key_file}')
            # load df to bigquery
            table_id = f'{client.project}.Data.Companies'
            load_dataframe_to_bigquery(client, table_id, company_df)

        
    # task dependdency
    company_list = extract_company_task()
    company_list = transform_company_task(company_list)
    load_company_task(company_list)


company_dag()