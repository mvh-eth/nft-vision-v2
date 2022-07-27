import os
import time

from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
from mangum import Mangum

#from src.database import Database
from src.database_bigquery import BigQuery
from src.reservoir import Reservoir
from src.scheduled_run import *

stage = os.environ.get('STAGE', None)
openapi_prefix = f"/{stage}" if stage else "/"

app = FastAPI(title="NFTVision", openapi_prefix=openapi_prefix) # Here is the magic

#db = Database()
bq = BigQuery()

reservoir = Reservoir()

@app.on_event("startup")
#repeat_every(seconds=60 * 15)  # repeat 15 mins
def refresh():
    
    update_top_collections()
    
    #read all top collections
    top_collections = bq.read_mongo("top_collections", return_type="dict")
    
    for collection in top_collections:
        #start stop watch
        start = time.time()
        
        if not bq.collection_exists(f"{collection['slug']}_metadata"):
            print(f"{collection['slug']}_metadata does not exist")
            #update_metadata(collection)
            
        update_sales(collection)
        update_listings(collection)
        
        #stop stop watch
        end = time.time()
        
        print(f"updated {collection['slug']} in {end - start} seconds")
    
    print(f"updated all data")

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/top-collections")
async def get_top_collections():
    return db.read_mongo("top_collections")

#add a route to get a collection by slug
@app.get("/collection/{slug}")
async def get_collection(slug: str):
    return db.read_mongo("collections", query_filter={"slug": slug})

#add a route to get sales data by collection slug
@app.get("/collection/{slug}/sales")
async def get_collection_sales(slug: str):
    return db.read_mongo(f"{slug}_sales", query_sort=[("timestamp", -1)], query_limit=100)

#add a route to get listings data by collection slug
@app.get("/collection/{slug}/listings")
async def get_collection_listings(slug: str):
    return db.read_mongo(f"{slug}_listings", query_sort=[("createdAt", -1)], query_limit=100)


handler = Mangum(app)

''' #run main
if __name__ == "__main__":
    uvicorn.run(app, debug=True) '''


