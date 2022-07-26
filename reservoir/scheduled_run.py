import os
import time

from database import Database
from dotenv import load_dotenv
from pandas import to_datetime

from reservoir.reservoir import Reservoir

load_dotenv()  # take environment variables from .env.

db = Database()

reservoir = Reservoir()

def update_sales(collection):
    collection_contract = collection['primaryContract']
    
    table_name = f"{collection['slug']}_sales"
    
    #get todays date as epoch
    today = int(time.time())
    
    #check collection exists in database
    if not db.collection_exists(table_name):
        #get 1000 days ago as epoch
        start_date = today - (60 * 60 * 24 * int(os.getenv("MAX_SALES_DAYS")))
        last_sale = start_date
    else:
        #get the last sale from the database
        last_sale = db.read_mongo(table_name, query_sort=[("timestamp", -1)], query_limit=1)[0]['timestamp']
        
        last_sale = int(last_sale) + 1

    resp = reservoir.get_historical_sales(collection=collection_contract, start_date=last_sale, end_date=today)
    
    if len(resp['sales']) == 0:
        return None

    db.write_mongo(table_name, resp['sales'])

    #if continuation token is present, keep calling the API until no continuation token is present
    while resp['continuation']:
        resp = reservoir.get_historical_sales(collection=collection_contract, start_date=last_sale, end_date=today, continuation=resp['continuation'])
        db.write_mongo(table_name, resp['sales'])
    
    print(f"updated {collection['slug']} sales data")
    
    return None
    
def update_listings(collection):
    
    collection_contract = collection['primaryContract']
    
    table_name = f"{collection['slug']}_listings"
    
    if db.collection_exists(table_name):

        #get the last listing from the database
        last_listing = db.read_mongo(table_name, query_sort=[("createdAt", -1)], query_limit=1)[0]['createdAt']
        
        last_listing = to_datetime(last_listing)
        
        #make sure last listing is over 10 mins ago
        if (time.time() - last_listing.timestamp()) > (60 * 10):
            print(f"listings up to date for {collection['slug']}")
            return None

    resp = reservoir.get_active_listings(collection=collection_contract, continuation=None)
    
    if len(resp['orders']) == 0:
        return None

    db.write_mongo(table_name, resp['orders'])

    #if continuation token is present, keep calling the API until no continuation token is present
    while resp['continuation']:
        resp = reservoir.get_active_listings(collection=collection_contract, continuation=resp['continuation'])
        db.write_mongo(table_name, resp['orders'])
    
    print(f"updated {collection['slug']} listings data")
    
    return None

def update_metadata(collection):
    table_name = f"{collection['slug']}_metadata"

    resp = reservoir.get_metadata(collection=collection['primaryContract'])
    tokens = [i['token'] for i in resp['tokens']]
    db.write_mongo(table_name, tokens)
    
    #if continuation token is present, keep calling the API until no continuation token is present
    while resp['continuation']:
        resp = reservoir.get_metadata(collection=collection['primaryContract'], continuation=resp['continuation'])
        tokens = [i['token'] for i in resp['tokens']]

        db.write_mongo(table_name, tokens)

    print(f"updated {collection['slug']} metadata")
    
    return None