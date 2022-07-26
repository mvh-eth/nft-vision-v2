import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


class Database(object):
    """
    Class to handle all of the api calls from the database.
    """

    def __init__(self):
        self.url = os.getenv("MONGO_URL")
        self.database_name = os.getenv("MONGO_DATABASE_NAME")
        
        my_client = MongoClient(self.url)
        self.db = my_client[self.database_name]


    def write_mongo(
        self, collection, data, overwrite=False,
    ):
        """Wrapper for the mongoDB .insert_many() function to add to the database
        Args:
            collection ([str]): name of the mongoDB collection
            data ([dataframe or list of dict]): [description]
            overwrite (bool, optional): [description]. Defaults to False.
        """
        
        my_collection = self.db[collection]

        if overwrite:
            n_docs = my_collection.count_documents({})
            print(f"deleting {n_docs} from {collection} mongoDB collection.")
            my_collection.delete_many({})  # delete all data

        if isinstance(data, pd.DataFrame):
            # convert dataframe back to list of dictionaries
            data = data.to_dict("records")

        # write data to collection
        if len(data) > 1 and isinstance(data, list):
            try:
                my_collection.insert_many(data, ordered=False)

                print(f"updating {collection} with {len(data)} documents.")

            except Exception as e:
                return e
            
            
    def read_mongo(self,
               collection,
               return_id=False,
               query_filter = None,
               query_projection = None,
               query_sort = None, 
               query_limit=None, 
               return_df=False,
               skip=None):

        """ Wrapper for the mongoDB .find() function to read from the database

        Returns:
            [list]: list of dictionaries
        """
        if query_filter is None:
            query_filter = {}
        if query_projection is None:
            query_projection = []
        if query_sort is None:
            query_sort = []

        my_collection = self.db[collection]

        # if no projection input, defult to all columns
        if len(query_projection) < 1:
            query_projection = my_collection.find_one().keys()

        if return_id is False and not isinstance(query_projection, dict):
            query_projection = dict.fromkeys(query_projection, 1)
            query_projection["_id"] = 0

        # if no limit input, set to all documents
        if query_limit is None:
            query_limit = my_collection.count_documents({})

        # Make a query to the specific DB and Collection
        if skip and int(skip):
            data = list(
            my_collection.find(
                filter=query_filter,
                projection=query_projection,
                sort=query_sort,
                limit=query_limit,
            ).skip(skip)
        )
        else:
            data = list(
            my_collection.find(
                filter=query_filter,
                projection=query_projection,
                sort=query_sort,
                limit=query_limit,
            )
        )

        if data:
            if return_df:
                data = pd.DataFrame(data)
            return data
        else:  # return None if no data found
            print(f"No data found for {collection} with specific query - {query_filter}")
            return None

    def collection_exists(self, collection):
        """Check if a collection exists in the database
        Args:
            collection ([str]): name of the mongoDB collection
        Returns:
            [bool]: True if collection exists, False if not
        """
        my_collection = self.db[collection]
        return my_collection.count_documents({}) > 0
