from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
import pandas as pd 
from datetime import datetime
import time 
import sys 
from selenium.common.exceptions import NoSuchElementException
import logging

service = Service(r'C:\selenium_driver\chromedriver') 
options  = webdriver.ChromeOptions()
options.add_argument('headless')
driver = webdriver.Chrome(options=options, service=service)
logging.getLogger("elasticsearch").setLevel(logging.ERROR)


url = 'https://db-engines.com/en/systems'
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
        # return None


def elastic_insert(es:Elasticsearch ,value :dict, index_name):

    try:
            
        # _id_ =  value['company'].replace(' ','-') + '-'+str(value['date'])
        _id_ =  value['database_name'].lower()
        es.index(index=index_name,id=_id_,  body=value)
    except KeyError as e:
        print(f"id field gave an error, {e.__class__.__name__}")
        pass
    
def createOrIgnoreIndex(es,index_name):

    if  es.indices.exists(index=index_name):
        print(f"Index {index_name} already exists")

        
    else:
        es.indices.create(index=index_name)
        print(f"The index '{index_name}' has been created in Elasticsearch.")

def current_fix(value):
    value  = value.rsplit(',',1)
    
    if type(value) ==  list and len(value)>1 :
        if len(value[1].split()) > 1: #if month name and year are included 
            
            _ = value[1].strip()
            fmt = "%B %Y"
            converted_date = datetime.strptime(_,fmt)
            return converted_date
        
        else:
            return datetime(int(value[1].strip()),1,1)
            
    
    
    else: 
        fmt = "%B %Y"
        return  datetime.strptime(value[0].strip(),fmt)
        
        
        


def parse_date(date_str):
    try:
        # Try parsing using 'm/d/Y' format
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except ValueError:
        # If that fails, try parsing using 'd/m/Y' format
        return datetime.strptime(date_str, '%m/%d/%Y').date()
        
if __name__  == '__main__':
    
    
    # # es_host = "#######"
    # # es_port = '###'
    # # es_user='elastic'
    # # es_pass=######'

    es = es_connector(es_host,es_port,es_user,es_pass)
    
    
    index_name ="tdap_db_engines_testing"
    es_connected =  False 


    if es:
        createOrIgnoreIndex(es,index_name)
        es_connected = not es_connected
    else:
        print("unable to connect to elastic search")
    
    driver.get(url)
    
    final_details = []
    
    step1 = driver.find_element(By.CLASS_NAME, 'list')
    columns =  step1.find_elements(By.TAG_NAME,'td')
    links = []
    for column in columns:
        
        for row in column.find_elements(By.TAG_NAME,'a'):
            
            links.append(row.get_attribute('href'))
    
    test =10
    
    for _idx, link in  enumerate(links):
        
        
        
        driver.get(link)
        
        counter =0
        
    #     for 
        
        tools = driver.find_element(By.CLASS_NAME,'tools')
        # details =  tools.find_elements(By.TAG_NAME,'tr')
        # details =  tools.find_elements(By.CSS_SELECTOR,'tbody >tr')
        details =  tools.find_elements(By.XPATH,'./tbody/tr')
        
        # check first if it has attribute
        # first value does 
        # second ovalue has the class of header which is the name of the database
        
        disposeable = {}
        details.pop(0) 
        
        for val in details :
            
            try:
                
                _left = val.find_element(By.CLASS_NAME,'attribute').text
                
                try:
                    _right =  val.find_element(By.CLASS_NAME,'value').text 
                
                
                except NoSuchElementException:
                    # print("first exceptin raised")
                    try:
                        _right =  val.find_element(By.CLASS_NAME,'header').text
                    
                    except NoSuchElementException:
                        
                        print("breaking away ")
                        
                        break 
                # changed=True
                if 'db-engines ranking' in  _left.lower() :
                    _left = ''

                    pass 
                                    
                elif 'website'  in _left.lower():
                    _right = val.find_element(By.CLASS_NAME,'value').find_elements(By.TAG_NAME,'a')#.get_attribute('href')
                    
                    if _right:
                        
                        if len(_right)>1:
                            _= []
                            for i in _right:
                                _.append(i.get_attribute('href'))
                            
                            _right = _ 
                        else:
                            
                            _right =  _right[0].get_attribute('href')
                            
                    else:
                        _right = val.find_element(By.CLASS_NAME,'value').text
                        
                    
                    
                elif 'server operating systems' in _left.lower():
                    _right = _right.replace('z/','').split('\n')
                    _left = _left.replace(' ','_') 
                
                elif 'description' in _left.lower():
                    _left = 'doc_text'

                elif 'initial release' in _left.lower():
                    _left =  'doc_date'
                    _right =  datetime(int(_right),1,1)
                    
                    # print("date found")

                elif 'primary database model' in _left.lower():
                    _left='database_model'
                    
                    
                    
                elif 'technical documentation' in _left:
                    _right  =  _right.replace('\xad','')
                    _left = _left.replace(' ','_')
                

                elif len(_left.split(' ')) >7:
                    _left='' 
                
                elif 'dbaas offerings' in _left.lower():
                    _left = _left.replace(' ','_')
                    pass 
                elif 'pgdash' in _left.lower():
                    _left = ''
                
                elif 'instaclustr' in _left.lower():
                    _left=''
                    
                elif 'supported programming languages' in _left.lower():
                    _right = _right.replace('z/','').split('\n')
                    _left = _left.replace(' ','_')
                    
                elif 'developer' in _left.lower():
                    _right = _right.split(',')
                    
                elif '' in _left:
                    pass
                elif '' in _right:
                    
                    pass

                # else:
                #     disposeable[_left.lower()] = _right if type(_right) == list else  _right.lower()
                    # changed =  not changed
                    # final_details.append({_left:_right})
                # input("press enter to continue")
                # print(_left)
                # if _left.lower() == 'initial release':
                #     _left =  'database_model'
                #     disposeable[_left.lower()] =  _right
                # else:
                if _left.lower() == 'doc_date':
                    
                    disposeable[_left] =_right 
                else:
                    disposeable[_left.lower()] =_right if type(_right) == list else  _right.lower()
                # if  changed:
                # else:
                #     disposeable[_left.lower()] =_right if type(_right) == list else  _right.lower()
                
                
                
            except NoSuchElementException:
                print("final Exceptin raised")
                counter +=1 
                
                if counter  >=3:
                    
                    counter = 0
                    break 
        # print(disposeable)
        # input('press enter')
        try:
            disposeable['database_name'] =  disposeable.pop('name')
            
        except:
            pass 
        
        try:
            disposeable['current_release'] = current_fix(disposeable.pop('current release'))
        
        except:
            disposeable['current_release']=datetime(1970, 1, 1)
            pass 
        
        # fixing implementation_language
        try:
            disposeable['implementation_language'] = disposeable.pop('implementation language')
            
            if 'and' in disposeable['implementation_language']:
                disposeable['implementation_language'] = disposeable['implementation_language'].split('and')
                
            else:
                disposeable['implementation_language'] = disposeable['implementation_language'].split(',')
        except:
            pass

        try:
            disposeable.pop('')
        except:
            pass

        try:
            disposeable['load_date'] =  datetime.now()
            disposeable['url'] =  link
            disposeable['initial_release'] =  disposeable['doc_date']
            # 
            
            

        except Exception as e:
            print(f"last try broke {e.__class__.__name__}")
            pass 

        final_details.append(disposeable)
        # disposeable.clear()

        print(f"now in {link}")

        # if _idx == test:
    
        #     break 
    print("please Wait inserting into elastic search")
    if es_connected:
        for _x ,inj in  enumerate(final_details):
            
            elastic_insert(es,inj,index_name=index_name)
        
        print("Insertion Complete")
                
            
        
# how to select only direct child tag and not grandchild tags in selenium

# tools.find_elements(By.CSS_SELECTOR,'tbody >tr')
                
                
            
        
        
        
        
        
        
            
            
            
        