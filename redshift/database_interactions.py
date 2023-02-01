import redshift_connector as rsc
import pandas as pd


# JDBC jdbc:redshift://default.020428635727.eu-west-2.redshift-serverless.amazonaws.com:5439/dev
# ODBC Driver={Amazon Redshift (x64)}; Server=default.020428635727.eu-west-2.redshift-serverless.amazonaws.com; Database=dev


conn = rsc.connect(
     host='default.020428635727.eu-west-2.redshift-serverless.amazonaws.com',
     database='dev',
  
  )

print(conn)
