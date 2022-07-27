"""
    create a bigquery class with the following methods:
    - create_table
    - insert_data
    - read_data
    
    each method should use pandas dataframes to interact with the bigquery API
"""
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

class BigQuery:
    def __init__(self):
        self.key_path = "nft-vision.json"
        self.credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id,)
        
    def write_dataframe_to_table(self, dataframe, table_name):
        """
        Write a dataframe to a bigquery table
        Args:
            dataframe (pandas.DataFrame): dataframe to write to table
            table_name (str): name of table to write to
        """
        # Load data to BQ
        job = self.client.load_table_from_dataframe(dataframe, table_name)
        job.result()
        print(f"Loaded {job.output_rows} rows into {self.credentials.project_id}:{table_name}.")
        
    def read_dataframe_from_table(self, table_name):
        """
        Read a dataframe from a bigquery table
        Args:
            table_name (str): name of table to read from
        """
        # Read data from BQ
        table = self.client.get_table(table_name)
        return self.client.list_rows(table).to_dataframe()
    
    def check_table_exists(self, table_name):
        """
        Check if a table exists in the bigquery database
        Args:
            table_name (str): name of table to check
        """
        return self.client.get_table(table_name) is not None