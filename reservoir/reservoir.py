
import requests

class Reservoir:
    """
    This class handles the API calls to the reservoir API.
    """
    def __init__(self):
        self.base_url = "https://api.reservoir.tools"
        self.headers = {"Accept": "*/*", "x-api-key": "demo-api-key"}

    def get_top_collections(self, top_n=20):
        url = f"{self.base_url}/collections/v4?sortBy=30DayVolume&includeTopBid=false&limit={top_n}"
        top_collections = requests.get(url, headers=self.headers).json()
        return top_collections["collections"]
    
    #Bulk historical sales data
    def get_historical_sales(self, collection, start_date, end_date, continuation=None):
        url = f"https://api.reservoir.tools/sales/bulk/v1?contract={collection}&limit=1000&startTimestamp={start_date}&endTimestamp={end_date}"
        if continuation:
            url += f"&continuation={continuation}"
        return requests.get(url, headers=self.headers).json()

    def get_active_listings(self, collection, continuation=None):
        url = f"https://api.reservoir.tools/orders/asks/v2?contracts={collection}&sortBy=createdAt&limit=1000"
        if continuation:
            url += f"&continuation={continuation}"
        return requests.get(url, headers=self.headers).json()



