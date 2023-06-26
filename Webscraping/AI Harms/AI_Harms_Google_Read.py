from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException,InvalidArgumentException
from selenium.webdriver.support import expected_conditions as EC
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk 
from elasticsearch.exceptions import RequestError
import pandas as pd 
from datetime import datetime
import time ,sys ,gspread
import re , os
import configparser

# link to file
url = 'https://docs.google.com/spreadsheets/d/1Bn55B4xz21-_Rgdr8BBb2lt0n_4rzLGxFADMlVW0PYI/edit#gid=888071280'
# functin that connects to elastic search 
def es_connector(es_host, es_port, es_user, es_pass):
    # connector to ELK as 02042021
    client = Elasticsearch(
        [es_host],
        http_auth=(es_user, es_pass),
        scheme="https",
       port=es_port,
      # ssl_context=context,
        timeout=100,verify_certs=False
    )
    if client.ping():
        print("Connected to ES\n")
        return client
    else:
        sys.exit("Failed connection to ES")
        return None
# create an index if it doesnt exist, ignore if it does
def createOrIgnoreIndex(es,index_name):

    if  es.indices.exists(index=index_name):
        print(f"Index {index_name} already exists")

        
    else:
        es.indices.create(index=index_name)
        print(f"The index '{index_name}' has been created in Elasticsearch.")

# formats the date file into pythondates
def parse_date(date_str):
    try:
        # Try parsing using 'm/d/Y' format
        return datetime.strptime(date_str, '%m/%d/%Y').date()
    except ValueError:
        # If that fails, try parsing using 'd/m/Y' format
        return datetime.strptime(date_str, '%d/%m/%Y').date()

# formats the date field into python dates
def doc_date_parse(val):
   
    if val['released_date']:
        date_object = datetime.strptime(val.released_date+'-1-1',"%Y-%m-%d")
        
    elif val['occurred_date']:
        date_object = datetime.strptime(val.occurred_date+'-1-1',"%Y-%m-%d")
        
    else:
        date_object = datetime.strptime('1970-1-1',"%Y-%m-%d")
    
    return date_object

# def punct_split_maker(string):
    
#     split_string = re.split(r'[^\w\s]', string)
#     word_list = list(filter(lambda word: word != '', split_string))
#     word_list = [word.strip() for word in word_list]
#     return word_list

# removes the pictuatin marks in a string
def punct_split_maker(string):
    split_string = re.split(r'[^\w\s]', string)
    split_string = [s for s in split_string if s]  # Remove empty strings

    if len(split_string) ==0:
        return split_string
    
    
    elif len(split_string) == 1:
        return split_string[0]
    
    
    else:
        return split_string

# Elastic search bulk insert  for dataframe 
def pandasBulkInsert(dataframe,id_col):
    # id_record =  dataframe.pop("aiaaic id#")
    # the reason fo this poping is so that it gets excluded  as a vairble and get treated as an id only
    id_record =  dataframe.pop(id_col)
    
    data = df1.to_dict(orient="records")
    actions = [
    {
        "_index": index_name,
        "_id": id_record.iloc[idx],  # Specify your own _id field from the DataFrame if not using it specify the column name 
        "_source": record
    }
    for idx,record in enumerate(data)
]
    
    bulk(es,actions)
    
    print("Insertion Complete")

# this takes the individual link and extracts the doc text from the link 
def get_doc_text(link):

    print(f'currently on  link---> {driver.current_url}')
    # print(f"index {idx}")
    if not link:
        return ""
    
    elif 'aiaaic' not in link:
        return link
    else:
        
        
        driver.get(link)
    
    
        try:
            mother = driver.find_element(By.XPATH,'//*[@class="UtePc RCETm sPG4ze yxgWrb"]')
            # Raw Text
            doc_text =  mother.find_elements(By.TAG_NAME,'section')[2].text
        
            # cleaned Text
            doc_text = doc_text.replace("'", "")
            doc_text = doc_text.replace('\n', '')
            
            final = ' '.join(doc_text.split())

            print("Done")
            return final 
        
        except NoSuchElementException:
        
        
            reason = driver.find_element(By.CLASS_NAME,'NVoTp').text 
            
            if reason:
                
                return reason+' Page Not found'
        
    
        
         
            
# proxy 
os.environ["http_proxy"] = "http://proxy-chain.intel.com:911"
os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"
    
if __name__ == '__main__':
    
    
   
    es_host = "###"
    es_port = '#####'
    es_user='##########c'
    es_pass='######'

    # Connecting to Elastic search 
    es = es_connector(es_host,es_port,es_user,es_pass)
    
    
    index_name ="tdap_ai_harms"
    es_connected =  False 


    if es:
        createOrIgnoreIndex(es,index_name)
        es_connected = not es_connected
    else:
        print("unable to connect to elastic search")

    
    
    
    # url to spreadsheet
    sheet_url =  'https://docs.google.com/spreadsheets/d/1Bn55B4xz21-_Rgdr8BBb2lt0n_4rzLGxFADMlVW0PYI/edit#gid=888071280'
    url_1 = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')
    
    document = 'authenticator.json'
    
    # document id connecting using gspread 
    document_id = '1Bn55B4xz21-_Rgdr8BBb2lt0n_4rzLGxFADMlVW0PYI'
    gc = gspread.service_account(document)
    
    spreadsheet = gc.open_by_key(document_id)
    
    worksheet = spreadsheet.worksheet('Repository')
    data =worksheet.get_all_values()
    
    # parse data into dataframe
    df = pd.DataFrame(data[3:], columns=data[1])
    
    df1 = df[['AIAAIC ID#', 'Title', 'Type', 'Released', 'Occurred', 'Country(s)',
       'Sector(s)', 'Operator(s)', 'Developer(s)', 'System name(s)',
       'Technology(ies)', 'Purpose(s)', 'Media trigger(s)', 'Risks(s)',
       'Transparency','Summary/links']]
    
    # renaming columns passed into the dataframe
    df1.rename(columns ={'Summary/links':'url','Occurred':'Occurred_date','Released':'Released_date','Country(s)':'Country',
       'Sector(s)':'sector', 'Operator(s)':'operator', 'Developer(s)':'developer', 'System name(s)':'system_name',
       'Technology(ies)':'technology', 'Purpose(s)':'purpose', 'Media trigger(s)':'media_trigger', 'Risks(s)':'risk'},inplace=True)
    
    df1 = df1.rename(columns=lambda x: x.lower())
    
    
    df1['doc_date'] = df1.apply(doc_date_parse,axis=1)
    df1['load_date'] =  datetime.now()
    
    _ = df1[['country',
       'sector', 'operator', 'developer', 'system_name',
       'technology', 'purpose', 'media_trigger', 'risk','transparency']]
    
    
    df1[_.columns] = _.applymap(punct_split_maker)

    # deleted the proxy because it was obstructing  selenium from running
    del os.environ["http_proxy"]
    del os.environ["https_proxy"]
    service = Service(r'C:\selenium_driver\chromedriver') 
    options  = webdriver.ChromeOptions()
    # options.add_argument('headless')
    driver = webdriver.Chrome(options=options, service=service)
    
    df1['doc_text'] = df1.url.apply(get_doc_text)
    
    # bulk inserts  dataframe more efficiently
    pandasBulkInsert(df1,"aiaaic id#")
    
    
   
    
    
    
    
        
    