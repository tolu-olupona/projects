from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
import pandas as pd 
from datetime import datetime
import time 
import sys 
from selenium.common.exceptions import NoSuchElementException
import logging

service = Service('/home/amiltra/Webdriver/chromedriver') 
options  = webdriver.ChromeOptions()
options.add_argument('headless')
driver = webdriver.Chrome(options=options, service=service)



logging.getLogger("elasticsearch").setLevel(logging.ERROR)

# not all tags were assigned classes , thats why xpath was used
def document(url):

    global driver ,ab,junk,_, est
       

    try:
               
        try:
            
            driver.find_element(By.XPATH, '//*[@id="submission-image-slider"]/div[1]/div/img')
    
            keywords =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[8]').text
            subject =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[9]').text
        
        
        except NoSuchElementException as e:
                keywords =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[7]').text
                subject =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[8]').text


        title =  driver.find_element(By.XPATH, "//h1[@class='show-title']").text
        authors =  driver.find_element(By.XPATH, "//div[@class='manuscript-authors']").text
        # lead_author =  [i.replace('*','').strip() for i in authors.split(',') if "*" in i]
        authors = [i.replace('*','').strip() for i in authors.split(',')]

        est =  str([{'author':authors,'lead_auth':lead_author}])
        # print(est)
        doc_date = driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[3]').text
        _ =  doc_date
        if 'CEST' in doc_date and 'CET' in doc_date:
            doc_date =  doc_date.split("Online:")[-1]
            if 'CET' in doc_date:
                doc_date = doc_date.replace("(","").replace(")","").replace("CET","").strip()
                doc_date =  datetime.strptime(doc_date,"%d %B %Y %H:%M:%S")
            elif 'CEST' in doc_date:
                doc_date = doc_date.replace("(","").replace(")","").replace("CEST","").strip()
                doc_date =  datetime.strptime(doc_date,"%d %B %Y %H:%M:%S")
            

        elif 'CEST'  in doc_date:
            doc_date = doc_date.rsplit("/",1)[1].split(":",1)[1].replace("(","").replace(")","").replace("CEST","").strip()
            ab =  doc_date
            doc_date =  datetime.strptime(doc_date,"%d %B %Y %H:%M:%S")
        else:
            doc_date = doc_date.rsplit("/",1)[1].split(":",1)[1].replace("(","").replace(")","").replace("CET","").strip()
            junk =  doc_date
            print('got here')

            doc_date =  datetime.strptime(doc_date,"%d %B %Y %H:%M:%S")
   
        views = driver.find_element(By.XPATH, "//span[@class='count view-number']").text
        preprint_type = driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[1]/span[2]').text
        downloads = driver.find_element(By.XPATH, "//span[@class='count download-number']").text
        comments =driver.find_element(By.XPATH, "//span[@class='count']").text
        version_no =  driver.find_element(By.XPATH, "//span[@class='version-span']").text
        # load_date = datetime.strptime(time.ctime(), '%a %b %d %H:%M:%S %Y').strftime('%d %B %Y %H:%M:%S')
        load_date =  datetime.now()
        _id_ =  driver.current_url.split("/")[-2]


        doc_text =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/div[6]').text






    except NoSuchElementException as e:
        if driver.find_element(By.XPATH , '//*[@id="submission-content"]/i/i'):
            doc_text = driver.find_element(By.XPATH, '//*[@id="submission-content"]/i/i/div[1]').text

            keywords =  driver.find_element(By.XPATH, '//*[@id="submission-content"]/i/i/div[2]').text
            subject = driver.find_element(By.XPATH , '//*[@id="submission-content"]/i/i/div[3]').text
    
    
    print("document function successfully returned")
    print()
    return {"title":title,'authors':est,'subject':subject,'keywords':keywords,'doc_text':doc_text,"doc_date":doc_date,'views':views,'downloads':downloads,'comments':comments,'url':url, 'version_no':version_no,'load_date':load_date,'_id_':_id_,'preprint_type':preprint_type}

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

def createOrIgnoreIndex(es,index_name):

    if  es.indices.exists(index=index_name):
        print(f"Index {index_name} already exists")

        
    else:
        es.indices.create(index=index_name)
        print(f"The index '{index_name}' didnt exist but now has been created in Elasticsearch.")

        
        

# checks if id exists
def id_exist(index_name:str,doc_id:str):
    global es
    search_query = {
        "query": {
            "term": {
                "_id": {
                    "value": doc_id
                }
            }
        }
    }
    result = es.search(index=index_name, body=search_query)
    return result

def update_record(details):
    # if you want to update all fields
    # for keys in details.keys():
    #     if keys =='_id':
    #         pass
    #     else:
    #         es["_source"][keys] = details[keys]
    fields = ["views","comments","downloads",'load_date']
    doc = es.get(index=index_name, id=details['_id_'])

    update_count = 0
    
    for name in fields:
        if doc["_source"][name] == details[name]:

            pass
        else:
            doc["_source"][name] = details[name]
            update_count +=1

    doc["_source"]['load_date'] =details['load_date']
    es.update(index=index_name, id=details['_id_'], body={"doc": doc["_source"]})

    if update_count :
        print(f"Total of {update_count} field was updated")
    else:
        print("Everything is  up to date ")
        print()
    
    
    print("Done Updating")
    print()



    # for hit in results['hits']
if __name__ == '__main__':
    currentPage =1
    
    baseurl = 'https://www.preprints.org/search?field1=title_keywords&field2=authors&clause=AND&search1=&search2=&page_num='

    url =  baseurl+str(currentPage)
    driver.get(url)

    totalPage = driver.find_element(By.XPATH, "//*[@id='main-content']/div[3]/div/div[1]/div/div/div[2]/div/div[2]/ul/li[5]/span").text


    #es Elasticclient object
	#connect herer via the Elastic object creator
    if es:
        COR = createOrIgnoreIndex(es,index_name)
        
        while currentPage <= int(totalPage):
            
            pg_links  = driver.find_elements(By.XPATH, "//*[contains(@class,'search-content-box') and contains(@class,'margin-serach-wrapper-left')]")

            link_list = []

            for i in pg_links:
              try:
                # link_list.append(i.find_element(By.XPATH, './/*[@class="content-box-header-element-5"]/a').get_attribute("href"))
                link_list.append(i.find_element(By.XPATH, './/*[@class="title"]').get_attribute("href"))

              except NoSuchElementException as e:
                sys.exit()
                print("tag ")
            __ = link_list
            for idx,lnk in  enumerate(link_list, start=1):
                
                driver.get(lnk)
                
                try:

                    lnk_details =  document(lnk)

                    testing =  lnk_details.copy()
                    update_det =  lnk_details.copy()
                    __id =  lnk_details.pop('_id_')
            

                    print(f"Currently at  page {currentPage}".center(100,'-'))
                    print(f"Line {idx}".center(100,'*'))
            
                    es.index(index=index_name, id=__id, body=lnk_details)
                    print(f"New Document Indexed  with id {__id}")
                
                
                except NoSuchElementException as e:
                    pass

            print(f"Done with page {currentPage}")
            currentPage +=1
            print(f"Traversing Page {currentPage}")
            next_page = baseurl+str(currentPage)
            driver.get(next_page)
       
    else:
        print("unable to connect check password and host")
