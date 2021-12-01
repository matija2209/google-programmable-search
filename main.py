import requests,json,pandas as pd,os,time
from googleapiclient.discovery import build
from db import get_db
from datetime import datetime
import pymongo,re

client = pymongo.MongoClient("mongodb+srv://matija:tofore@cluster0.u2hyx.mongodb.net/presentation_data?retryWrites=true&w=majority",tlsAllowInvalidCertificates=True)
presentation_db = client['presentation_data']

class GoogleSearch:
    
    def __init__(self,**kwargs):
        self.filter = 0
        self.search_results = list()
        self.my_api_key = os.getenv('G_API')
        self.my_cse_id = os.getenv('G_CSE')
        # https://programmablesearchengine.google.com/cse/setup/basic?cx=38a8ee8a282dd25b1
        self.kwargs = kwargs
        self.count = 0
        self.max_pages = 3
    
    def extract_search_results(self):
        #print(self.kwargs)
        service = build("customsearch", "v1", developerKey=self.my_api_key)
        page = 1
        total_results = list()
        while page <= self.max_pages:
            print(f'\t- visiting page: {page}')
            res = service.cse().list(cx=self.my_cse_id, **self.kwargs).execute()
            page_results = res['items']
            search_results = list(map(lambda x: {
                'title' : x['title'],
                'snippet' : x['snippet'],
                'url' : x['link'],#x['pagemap']['metatags'][0].get('og:url'),
                'kw' : self.kwargs['exactTerms'],
                "created_date": datetime.now()
            },page_results))
            total_results.append(search_results)
            try:
                self.kwargs['start'] = res['queries']['nextPage'][0]['startIndex']
            except:
                print("\t- no start index found")
                break
            page+=1
            time.sleep(2)
        return total_results

    def return_pandas_df(self):
        return pd.DataFrame(self.search_results)

def remove_null(data):

    def remove_api(ser):
        link = ser['url']
        if link is not None:
            return True
        else:
            return False
    non_none_list = list(filter(lambda url:remove_api(url),data))
    return non_none_list

def filter_irrelevant_results(data):

    def remove_non_three(url):
        actual_url = url['url']
        if actual_url.count('/') == 3 and re.search('\/$',actual_url):
            return True
        return False
    complete_domains = list(filter(lambda url:remove_non_three(url),data))
    return complete_domains

def competitor_exists_in_db(collection_name,new_data):
    collection = presentation_db[collection_name]
    entries_in_db = list(collection.find())

    def is_new(link):
        already_exists = next((sub for sub in entries_in_db if sub['url'] == link['url']), False)
        if already_exists:
            return False
        return True

    new_data = list(filter(lambda new_link:is_new(new_link),new_data)) 
    return new_data

def insert_new_rows(collection_name,new_data):
    collection = presentation_db[collection_name]
    collection.insert_many(new_data)
    return True


# https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list?hl=en
# gl="countryUK"
exact_terms = "corporate wellness consultants,wellness and performance company,corporate wellbeing consultants".split(",")
collection_name = "google_kw_data"

for search in exact_terms:
    print("processing ",search)
    query_object = GoogleSearch(exactTerms=search,lr="lang_en")
    initial_serp = query_object.extract_search_results()
    flattened_initial_serp = [x for x in initial_serp for x in x]

    non_null_serp = remove_null(flattened_initial_serp)
    relevant_serp = filter_irrelevant_results(non_null_serp)

    new_to_add = competitor_exists_in_db(collection_name,relevant_serp)
    if new_to_add:
        insert_new_rows(collection_name,new_to_add)
