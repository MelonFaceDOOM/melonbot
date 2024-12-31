from googleapiclient.discovery import build
import os
from config import gapikey, gcsekey
    
def search(search_term, api_key=gapikey, cse_id=gcsekey, **kwargs):
    service = build("customsearch", "v1", developerKey=api_key)
    res = service.cse().list(q=search_term, cx=cse_id, **kwargs).execute()
    return res
