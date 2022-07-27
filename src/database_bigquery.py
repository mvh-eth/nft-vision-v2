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
    def __init__(self, dataset_name="nft_vision"):
        self.key_path = "nft-vision.json"
        self.credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id,)
        self.dataset_name = dataset_name
        
    def write_mongo(self, table_name, dataframe, if_exists="append"):
        """
        Write a dataframe to a bigquery table
        Args:
            dataframe (pandas.DataFrame): dataframe to write to table
            table_name (str): name of table to write to
        """
        table_name = f"{self.dataset_name}.{table_name}"
        
        #check if dataframe is a dataframe
        if not isinstance(dataframe, pd.DataFrame):
            #try to convert to dataframe
            try:
                dataframe = pd.json_normalize(dataframe, sep="_")
            except:
                #if it fails, raise error
                raise ValueError("dataframe must be a pandas.DataFrame")
            
        if if_exists == "replace":
            #delete table if it exists
            if self.collection_exists(table_name):
                self.client.delete_table(table_name)
        
        # Load data to BQ
        try:
            job = self.client.load_table_from_dataframe(dataframe, table_name)
            job.result()
            print(f"Loaded {job.output_rows} rows into {self.credentials.project_id}:{table_name}.")
        except Exception as e:
            print(f"Error loading data into {self.credentials.project_id}:{table_name}.")
            print(e)
            return False
        
    def read_mongo(self, table_name, return_type="dataframe", query_sort=None, query_limit=None):
        """
        Read a dataframe from a bigquery table
        Args:
            table_name (str): name of table to read from
        """
        table_name = f"{self.dataset_name}.{table_name}"
        
        # Read data from BQ
        table = self.client.get_table(table_name)
        
        ''' if query_sort:
            data = self.client.list_rows(table, selected_fields=table.schema, order_by=query_sort)
        else:
            data = self.client.list_rows(table, selected_fields=table.schema) '''
        
        if return_type == "dataframe":
            return self.client.list_rows(table).to_dataframe()
        elif return_type == "dict":
            return self.client.list_rows(table).to_dataframe().to_dict(orient="records", )
        else:
            raise ValueError("return_type must be either 'dataframe' or 'dict'")
    
    def collection_exists(self, table_name):
        """
        Check if a table exists in the bigquery database
        Args:
            table_name (str): name of table to check
        """
        #if table name dosent have . in it, add it
        if "." not in table_name:
            table_name = f"{self.dataset_name}.{table_name}"
        #check if table exists
        try:
            self.client.get_table(table_name)
            return True
        except Exception:
            return False