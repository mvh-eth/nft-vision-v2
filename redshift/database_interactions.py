from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


key_path = "/Users/jeremysavage/Documents/MVH/nft-vision-v2/key.json"

credentials = service_account.Credentials.from_service_account_file(
 key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# Example data
df = pd.DataFrame({'a': [1,2,4], 'b': ['123', '456', '000']})

# Define table name, in format dataset.table_name
table = 'nft_vision.test_table'

# Load data to BQ
job = client.load_table_from_dataframe(df, table)

