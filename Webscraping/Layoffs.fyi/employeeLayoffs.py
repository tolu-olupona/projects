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

service = Service(r'C:\Users\tolupona\OneDrive - Intel Corporation\Desktop\Loader Project\Layoffs.fyi\chromedriver.exe') 
options  = webdriver.ChromeOptions()
options.add_argument('headless')
driver = webdriver.Chrome(options=options, service=service)

logging.getLogger("elasticsearch").setLevel(logging.ERROR)


url = 'https://airtable.com/shrMt3xmtTO0p4MGT/tblka5nfVbLBUTHo4?backgroundColor=green&viewControls=on'
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
        print(f"The index '{index_name}' has been created in Elasticsearch.")



def scroll(element, x,y,ori):
    action = ActionChains(driver)
    action.move_to_element(element)
    action.click_and_hold()
    # action.move_by_offset(x,y)
    action.scroll_from_origin(ScrollOrigin.from_element(ori),x,y)
    action.perform()
        
    action.reset_actions()

def scroll_h(element,x,y):
    action = ActionChains(driver)
    action.move_to_element(element)
    action.click_and_hold()
    action.move_by_offset(x,y)

    action.reset_actions()

def parse_date(date_str):
    try:

        return datetime.strptime(date_str,'%m/%d/%Y').date()
    except ValueError :
        return datetime.strptime(date_str,'%d/%m/%Y').date()
    
    
def elastic_insert(es:Elasticsearch ,value :dict, index_name):

    _id_ =  value['company'].split(' laid-off')[0].replace(' ','-') + '-'+str(value['layoff_date'])

    es.index(index=index_name,id=_id_,  body=value)

def company_clean(value):
    return value.replace(' laid-off employees','')

if __name__ == '__main__':
    
    
    driver.set_window_size(800,600)
    driver.get(url)

    print(f"connected to  {url}")

   
    
    genUrl =  driver.current_url
   
    TOP_LEVEL =  driver.find_element(By.CLASS_NAME,'nameAndDescription')

        
    vert =  driver.find_element(By.CLASS_NAME, 'antiscroll-scrollbar-vertical')
    hori =  driver.find_element(By.CLASS_NAME, 'antiscroll-scrollbar-horizontal')

    x_origin =  driver.find_element(By.CLASS_NAME, 'rowNumber')

   
    es_port = #####
    # es_host = "####"
    # es_port = '9200'
    # es_user='elastic'
    # es_pass='####'

    es = es_connector(es_host,es_port,es_user,es_pass)

    index_name ="tdap_tech_employee_layoffs"

    es_connected =  False 
    if es:
        createOrIgnoreIndex(es,index_name)
        es_connected = not es_connected
    else:
        print("unable to connect to elastic search")


    counter =1 
    broken =False 
    lp,rp = [],[]
    total_record = int(driver.find_element(By.CLASS_NAME, 'summaryCell').text.replace(' records',''))
    while True:
        # remember to return total_record
        # if counter-1 >300:
        #     break

        if broken:
            break 
        
        for idx,txt in enumerate(driver.find_elements(By.CLASS_NAME, 'leftPane')):
            
            if counter - 1 ==total_record:
                broken=  not broken 
                break 
            
            if txt.text:
                    cur_val  = txt.text.split("\n")
                    try:
                        
                        num = int (cur_val[0])

                        print(f" index is {num}  counter is {counter}")
                        if counter == num:
                            rightPane =  driver.find_elements(By.CLASS_NAME, 'rightPane')
                            # you could append the num to keep track of the variable
                            lp.append([txt.text.split("\n")[1], rightPane[idx].text])

                            scroll(hori,800,0,x_origin )

                            rightPane =  driver.find_elements(By.CLASS_NAME, 'rightPane')
                            lp[counter-1].append(rightPane[idx].text)

                            scroll(hori,-800,0,x_origin )
                            counter +=1


                    except ValueError as e:
                        print("Empty value passed")

        scroll(vert,0,150,TOP_LEVEL)

    print("left while loop")
    newFil = []
    for idx,i in enumerate(lp):
            
        if idx ==0:
            temp_ = i[1].split('\n')
            comp_ =  i[0]
            ls_ =  i[2].split("\n")
            ls_.pop(0)

            newFil.append([comp_,temp_, ls_])

        else:
            comp_ =  i[0]
            temp_ =  i[1].split('\n')[::-1]
            ls_ =  i[2].split('\n')[-1] #this is the date time only 
            newFil.append([comp_,temp_, ls_])
    
    final_dict = {}
    data = []
    for x_ , var in enumerate(newFil):

        if len(var[1]) >3:
            

            if var[1][0].isdecimal():
            
                final_dict['company'] =company_clean(var[0])
                final_dict['layoff_date'] =  parse_date(var[1][1])
                final_dict['people_laid_off'] = var[1][0]
                final_dict['link'] = var[1][-1]
                final_dict['doc_date']  = parse_date(var[2][0] if type(var[2])==list else var[2])

                final_dict['locations'] = var[1][2:-1]

                final_dict['load_date'] =  datetime.fromtimestamp(time.time())

            else:

                final_dict['company'] =company_clean(var[0])
                final_dict['layoff_date'] = parse_date(var[1][0])
                final_dict['people_laid_off'] = var[1][1]
                final_dict['link'] = var[1][-1]
                if type(var[2])==list:
                    try:
                        final_dict['doc_date']  =parse_date(var[2][0] if type(var[2])==list else var[2])
                    except ValueError:
                        final_dict['doc_date'] =parse_date(var[2][1] if type(var[2])==list else var[2])

                final_dict['locations'] = var[1][2:-1]

                final_dict['load_date'] =  datetime.fromtimestamp(time.time())
            
            if final_dict['link']:
                
                if 'http' not in final_dict['link']:
                    final_dict['link'],final_dict['locations'] = final_dict['locations'],final_dict['link']
           
            data.append(final_dict)
        
        else:

            final_dict['company'] =company_clean(var[0])
            final_dict['layoff_date'] = parse_date(var[1][0])
            final_dict['people_laid_off'] = var[1][1]
            final_dict['link'] = var[1][-1]
            final_dict['doc_date'] = parse_date(var[2][0] if type(var[2])==list else var[2])
            final_dict['locations'] = ''

            final_dict['load_date'] =  datetime.fromtimestamp(time.time())
            # final_dict['_index'] = index_name

            
            if final_dict['link']:
                

                if 'http' not in final_dict['link']:
                        final_dict['link'],final_dict['locations'] = final_dict['locations'],final_dict['link']
           
            
            data.append(final_dict)


        if es_connected:
            elastic_insert(es= es ,value= final_dict , index_name= index_name)
            print(f"document {x_} inserted successfully ")
            print( final_dict)
            print(f"{'-':^30}")


            