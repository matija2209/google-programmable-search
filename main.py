import requests,json,pandas as pd,os
from googleapiclient.discovery import build

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
            print(f'Visiting page: {page}')
            res = service.cse().list(cx=self.my_cse_id, **self.kwargs).execute()
            page_results = res['items']
            search_results = list(map(lambda x: {
                'title' : x['title'],
                'snippet' : x['snippet'],
                'url' : x['pagemap']['metatags'][0].get('og:url')
            },page_results))
            total_results.append(search_results)
            try:
                self.kwargs['start'] = res['queries']['nextPage'][0]['startIndex']
            except:
                break
            page+=1
        return total_results

    def return_pandas_df(self):
        return pd.DataFrame(self.search_results)

# https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list?hl=en
query_object = GoogleSearch(exactTerms="wellbeing and performance company",gl="countryUK",lr="lang_en")
query_object.extract_search_results()