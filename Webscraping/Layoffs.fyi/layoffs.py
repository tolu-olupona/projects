# REQUIRES SELENIUM VERSION GREATER THAN OR EQUAL TO 4.2
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
import pandas as pd 
import numpy as np 
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


url = 'https://airtable.com/shrqYt5kSqMzHV9R5/tbl8c8kanuNB6bPYr?backgroundColor=green&viewControls=on'
# connects to elastic search 
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
# creates the index if it exists or ignores it and moves on
def createOrIgnoreIndex(es,index_name):

    if  es.indices.exists(index=index_name):
        print(f"Index {index_name} already exists")

        
    else:
        es.indices.create(index=index_name)
        print(f"The index '{index_name}' has been created in Elasticsearch.")

# function incharge of scrolling
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

        return datetime.strptime(date_str,'%d/%m/%Y').date()
    except ValueError :
        return datetime.strptime(date_str,'%m/%d/%Y').date()
    

# inserts data into elatic search
# also responsible for constructing the id
def elastic_insert(es:Elasticsearch ,value :dict, index_name):

    try:
            
        _id_ =  value['company'].replace(' ','-') + '-'+str(value['layoff_date'])
        es.index(index=index_name,id=_id_,  body=value)
    except KeyError as e:
        pass

# function checks if new value exists on the site , if it does it updates 
# it updates only newly inputed data
def updateOrNot(queryRecords:int):
    #     GET /tdap_layoffs/_search
    
    query = {
    "query": { "match_all": {} }}

    go =  es.search(index=index_name, body=query)


    if go['hits']['hits']:
        
        query = {
        "query": { "match_all": {} },
        "sort": { "layoff_date": "desc" },
        "size": queryRecords
        }

        res = es.search(index=index_name, body=query)
        grouped_result=[]
        for __ in res['hits']['hits']:

            _d =  __['_source']['layoff_date']
            _c =  __['_source']['company']

            grouped_result.append(_c+str(_d))

        return  grouped_result

    else:
        return ""

# incharge of breaking down the already gotten data and reshaping it
def miniConverter(lp):

    
    newFil=[]
    
    
    for idx,i in enumerate(lp):


        if idx == 0:

            temp_ = i[2].split('\n')
            ls_ =  i[3].split("\n")
            ls_.pop(0)

            if 'Non-U.S.' in temp_:
                temp_.pop(temp_.index('Non-U.S.'))

                newFil.append([i[1],temp_, ls_])

            else:
                # temp_ =  temp_[::-1]
                # temp_.pop(-1)
                newFil.append([i[1],temp_, ls_])
        else:
            
            temp_ = i[2].split('\n')[::-1]

            ls_ =  i[3].split("\n") 
            islink=False
             

            for lnkidx,lnk in enumerate(ls_):
                if 'http' in lnk:
                    islink =  not islink
                    break 
                    
            if islink:
                ls_.pop(lnkidx)
                islink =  not islink

            if 'Non-U.S.' in temp_:
                temp_.pop(temp_.index('Non-U.S.'))

                newFil.append([i[1],temp_, ls_])
            else:
                # temp_ =  temp_[::-1]
                
                newFil.append([i[1],temp_, ls_])
        

    return newFil 


if __name__ == '__main__':
    
    # sets the window frame size
    driver.set_window_size(800,600)
    driver.get(url)

   

    # stores the current url of whtever page we are currently on
    genUrl =  driver.current_url

    
    # stores the total record available so we know how long to iterate for
    total_record =  int(driver.find_element(By.CLASS_NAME,'summaryCell').text.replace(' records','').replace(',',''))

    print(f'total number of records eqauls {total_record}')

    # switches to window of interst
   

    # if the url are the same that means the page didnt change, in that case do nothing
    
    if True:
        print("page Swapped")


        print(driver.get_window_size())
        

        TOP_LEVEL =  driver.find_element(By.CLASS_NAME,'nameAndDescription')

        
        vert =  driver.find_element(By.CLASS_NAME, 'antiscroll-scrollbar-vertical')
        hori =  driver.find_element(By.CLASS_NAME, 'antiscroll-scrollbar-horizontal')

        x_origin =  driver.find_element(By.CLASS_NAME, 'rowNumber')

        # connecting to elastic search
        
        

        # es_pass='#######'
        # es_host='#######m'
        # es_user='elastic'
        # es_port = #######

        es = es_connector(es_host,es_port,es_user,es_pass)

        index_name ="tdap_tech_layoffs"
        # index_name ="tdap_layoffs_test"
        es_connected =  False 
        if es:
            createOrIgnoreIndex(es,index_name)
            es_connected = not es_connected
        else:
            print("unable to connect to elastic search")

        lp,rp = [],[]   
        counter =1
        breaker=0 
        broken=False 
        query_no =20
        lastValue =  updateOrNot(query_no)

        # loop in charge of iteration
        while True:
            

            if broken:
                break 
            
            # gathers the list of companies on the left and matches them to their corresponding values
            for idx,txt in enumerate(driver.find_elements(By.CLASS_NAME, 'leftPane')):
                
                if lastValue and counter-1 == query_no:
                    # miniConverter(lp,forupdate=True)
                    broken = not broken
                    break 

                if counter-1 ==  total_record:
                    broken=  not broken 
                    break 
                
                if txt.text:
                    cur_val  = txt.text.split("\n")
                    try:
                        
                        num = int (cur_val[0])

                        # print(f" index is {num}  counter is {counter}")
                        if counter ==num:# num:
                            rightPane =  driver.find_elements(By.CLASS_NAME, 'rightPane')
                            lp.append([num,txt.text.split("\n")[1], rightPane[idx].text])

                            scroll(hori,800,0,x_origin )

                            rightPane =  driver.find_elements(By.CLASS_NAME, 'rightPane')
                            lp[counter-1].append(rightPane[idx].text)

                            scroll(hori,-800,0,x_origin )
                            # print(f" index is {num}  counter is {counter}")
                            print(f'currently on number {counter}')

                            counter +=1

                    except ValueError as e:
                        # print("Empty value passed")
                        pass



            # break
            breaker +=1             
            scroll(vert,0,350,TOP_LEVEL)
            
        print("left while loop")
        # padding the semi structured data into varaible newFil
        newFil = miniConverter(lp)

        # bucket to store the new intended data
        data = []
        testonce=True

    #   iterating through the semi structured data to  prepare the data for elstic search
        for x_,variable in enumerate(newFil):
            try:

                final_dict = {}
                if len(variable[1]) ==3:

                    final_dict['company']= variable[0]
                    final_dict['company_headquarters']= variable[1][0]
                    final_dict['num_of_staff_laidoff']  =  ''
                    final_dict['layoff_date']  = parse_date(variable[1][1])
    
                    final_dict['percentage_of_staff_laoff']  =  ''
                    final_dict['industry']  =  variable[1][2]
                    final_dict['url']  =  ''

                elif len(variable[1]) ==4:
                    print("laid off is missing and percentage is missing")

                    if '20' in variable[1][0] and variable[1][0].count('/')==2:
                        final_dict['layoff_date']  = parse_date(variable[1][0])
                        final_dict['percentage_laidoff']  =  variable[1][1].replace('%','')
                        final_dict['industry']  =  variable[1][2]
                        final_dict['url']  =  variable[1][3]

                        final_dict['company']= variable[0]
                        final_dict['company_headquarters']= ''
                        final_dict['num_of_staff_laidoff']  =  ''
                    else:

                        final_dict['company']= variable[0]
                        final_dict['company_headquarters']= variable[1][0]
                        final_dict['num_of_staff_laidoff']  =  ''
                        final_dict['layoff_date']  = parse_date(variable[1][1])
        
                        final_dict['percentage_laidoff']  =  ''
                        final_dict['industry']  =  variable[1][2]
                        final_dict['url']  =  variable[1][3]



                elif len(variable[1]) ==5:
                    pers_present = False 
                    date_present =  True
                    final_dict['company']= variable[0]
                    final_dict['company_headquarters']= variable[1][0]

                    final_dict['industry']  =  variable[1][3]
                    final_dict['url']  =  variable[1][4]

                    for i in variable[1]:
                        if '%' in i and 'http' not in i :

                            pers_present= not pers_present 

                            break
                    for i in variable[1]:
                        if '202' in i and i.count('/') ==2:
                            pass 
                        else:
                            date_present = not date_present

                    if date_present:


                        if '%' in variable[1][-2]:
                            
                            final_dict['company']= variable[0]
                            final_dict['company_headquarters']= variable[1][0]

                            final_dict['industry']  =  variable[1][3]
                            final_dict['url']  =  variable[1][4]

                            final_dict['num_of_staff_laidoff']  =  variable[1][1]
                            final_dict['layoff_date']  = parse_date(variable[1][2])
                            final_dict['percentage_laidoff']  =  variable[1][3].replace('%','')
                            final_dict['industry']  =  variable[1][3]

                        elif pers_present:
                            print("percentage is present that means laid off is absent")

                            final_dict['company']= variable[0]
                            final_dict['company_headquarters']= variable[1][0]

                            final_dict['industry']  =  variable[1][3]
                            final_dict['url']  =  variable[1][4]
                            final_dict['num_of_staff_laidoff']  =  ''
                            final_dict['layoff_date']  = parse_date(variable[1][1])
                            final_dict['percentage_laidoff']  =  variable[1][2].replace('%','')
                    
                        else:
                            print("do something to laid off ")
                            final_dict['company']= variable[0]
                            final_dict['company_headquarters']= variable[1][0]

                            final_dict['industry']  =  variable[1][3]
                            final_dict['url']  =  variable[1][4]

                            final_dict['num_of_staff_laidoff']  =  variable[1][1]
                            final_dict['layoff_date']  = parse_date(variable[1][2])
                            final_dict['percentage_laidoff']  =  ''

                    else:
                        pass 

                else:

                    print("role is complete")


                    if variable[1][2].isdecimal():

                        percent =  [i_ for i_ in variable[1] if '%' in i_ and len(i_) <=4] 

                        final_dict['company']  =  variable[0]
                        final_dict['company_headquarters']= variable[1][:2]
                        final_dict['num_of_staff_laidoff']  =  variable[1][2]
                        final_dict['layoff_date']  = parse_date(variable[1][3])

                        if percent: 

                            final_dict['percentage_laidoff']  =  variable[1][4].replace('%','')
                            final_dict['industry']  =  variable[1][-2]
                            final_dict['url']  =  variable[1][-1]
                        else:
                            final_dict['percentage_laidoff']  =  ''
                            final_dict['industry']  =  variable[1][-2]
                            final_dict['url']  =  variable[1][-1]
                    else:

                        final_dict['company']  =  variable[0]
                        final_dict['company_headquarters']= variable[1][0]
                        final_dict['num_of_staff_laidoff']  =  variable[1][1]
                        final_dict['layoff_date']  = parse_date(variable[1][2])
                        final_dict['percentage_laidoff']  =  variable[1][3].replace('%','')

                        final_dict['industry']  =  variable[1][4]
                        final_dict['url']  =  variable[1][5]
            
            
            
            # cleaning second level 
            

                if len(variable[2]) ==2:

                    final_dict['list_of_employees_laid_off'] = ''
                    final_dict['company_stage'] =  ''
                    final_dict['funds_raised'] = ''
                    final_dict['country'] = variable[2][0]
                    final_dict['doc_date'] = parse_date(variable[2][1])

                elif len(variable[2]) ==3:

                    final_dict['company_stage'] =  variable[2][0]
                    final_dict['funds_raised'] =  ''
                    final_dict['list_of_employees_laid_off'] = ''
                    final_dict['country'] = variable[2][1]
                    final_dict['doc_date'] = parse_date(variable[2][2])

                elif len(variable[2]) ==4:
                    dol_present=False 
                    for x in variable[2]:
                        if '$' in x:
                            dol_present = not dol_present 
                            break 

                    if dol_present:
                        final_dict['company_stage'] =  variable[2][0]
                        final_dict['funds_raised'] =  variable[2][1]
                        final_dict['list_of_employees_laid_off'] =''
                        final_dict['country'] = variable[2][2]
                        final_dict['doc_date'] = parse_date(variable[2][3])
                        
                    else:
                        final_dict['list_of_employees_laid_off'] =variable[2][0]
                        final_dict['company_stage'] =  variable[2][1]
                        final_dict['funds_raised'] =  ''
                        final_dict['country'] = variable[2][2]
                        final_dict['doc_date'] = parse_date(variable[2][3])

                else:

                    final_dict['list_of_employees_laid_off'] =variable[2][0]
                    final_dict['company_stage'] =  variable[2][1]
                    final_dict['funds_raised'] =  variable[2][2]
                    final_dict['country'] = variable[2][3]
                    final_dict['doc_date'] = parse_date(variable[2][4])

                final_dict['funds_raised']=final_dict['funds_raised'].replace('$','')
                
                try:
                    final_dict['funds_raised'] = int (final_dict['funds_raised'])

                except: 
                    final_dict['funds_raised'] = 0
                

                try:
                    final_dict['percentage_laidoff'] = int(final_dict['percentage_laidoff'])

                except:
                    final_dict['percentage_laidoff'] = 0

                try:
                    final_dict['num_of_staff_laidoff'] = int(final_dict['num_of_staff_laidoff'])

                except:
                    final_dict['num_of_staff_laidoff'] =0



                final_dict['load_date'] =  datetime.fromtimestamp(time.time())
            
            except:
                pass

            try:

                del final_dict['list_of_employees_laid_off']
            except:
                pass
            
            bo =  False 
            

           
            # checking status for elastic search connection and comapring the lastvalue in our database against the resent value on the website
            if es_connected:
                if lastValue:
                    # this checks if the value has been checked for updated field once 
                    if testonce:

                        for __x, _val  in enumerate(lastValue):
                            if _val == final_dict['company'] +str(final_dict['layoff_date']):
                                print("field is up to date")
                                bo= not bo 
                                break 

                            else:
                                elastic_insert(es ,final_dict , index_name)
                                print(f"document {__x} inserted successfully first else ")
                            
                        testonce =  not testonce
                          
                else:
                    elastic_insert(es ,final_dict , index_name)
                    print(f"document {x_} inserted successfully ")

            if bo:
                break 

            print( final_dict)
            print(f"{'-':^30}")
        
        