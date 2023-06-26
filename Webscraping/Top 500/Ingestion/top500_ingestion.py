

import requests
import pandas as pd
from datetime import datetime
import dateparser
from elasticsearch import Elasticsearch
import sys
from datetime import datetime
from ssl import create_default_context
import os
import warnings
import argparse
from dateutil.parser import parse
import configparser

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description='top500 list etl script.\n extracts transforms and loads top500 and green500 lists from top500.org.')
parser.add_argument("-es", "--Elasticsearch",help="Choose the enviroment that the data should be pushed to (Dev/Prod)",type=str, default="Dev")
parser.add_argument("-d","--date", help="Date Range Selector\n type (all) for all data \n for specific daterange use (startyear-endyear) ex.(2005-2006) \n for a specified year and month type (year-month) for example (2022-06)\n for an entire year type (year) for example (2022) \n Note: top500 lists are usually updated in June and November\n",type=str, default="all")
args = vars(parser.parse_args())
config = configparser.ConfigParser()
config.read("C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Loader Project/Top 500/Ingestion/top500_env.cfg")



def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

#handling input
if args["Elasticsearch"] not in ["Dev","Prod"]:
	sys.exit("environment {} incorrect choose between Dev or Prod".format(args["Elasticsearch"]))
if is_date(args["date"]) == False and "all" not in args["date"].lower():
  if "-" in args["date"]:
    date_arg =  args["date"].split("-")
    for x in date_arg:
      if is_date(x) == False:
        sys.exit("Year Format {} is wrong \n ex. year should look like 2022 or 2013 ".format(x))


date_arg = args["date"]
print("The data will be ingested to {}".format(args["Elasticsearch"]))

# context = create_default_context(cafile="C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/json bulk dashboard/cacerts.pem")

os.environ["http_proxy"] = "http://proxy-chain.intel.com:911"

os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"
# import required modules
es_host = config[args["Elasticsearch"]]["es_host"]
es_port = config[args["Elasticsearch"]]["es_port"]
es_user = config[args["Elasticsearch"]]["es_user"]
es_pass = config[args["Elasticsearch"]]["es_pass"]



#Handling Connection to Elastic Search 
def es_connector(es_host, es_port, es_user, es_pass):
    # connector to ELK as 02042021
    client = Elasticsearch(
        [es_host],
        http_auth=(es_user, es_pass),
        scheme="https",
       port=es_port,
      # ssl_context=context,
        timeout=100
    )
    if client.ping():
        print("Connected to ES\n")
        return client
    else:
        sys.exit("Failed connection to ES")
        return None

es_client = es_connector(es_host, es_port, es_user, es_pass)

#Handling Connection to top500.org
login = config["Top500Creds"]["username"]
password = config["Top500Creds"]["password"]
link    = 'https://top500.org/accounts/login/?next=/'
session = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Mobile Safari/537.36','Referer':'https://top500.org/accounts/login/?next=/',"Cookie":"_ga=GA1.2.2043079962.1662646073; _gid=GA1.2.1694037072.1662646073; __gads=ID=1bc1996a3429a685:T=1662642488:S=ALNI_MZdrRR6QypUfgiygZY8S8LNJJSvjA; django_language=en; csrftoken=e0Bg7MRtASDmj799aP7anmOg2KiB3hcYCKuzwzuz49McA6sWLx2SvucGnlAyhVF4; _gat_gtag_UA_325590_1=1; __gpi=UID=00000abd126c4a94:T=1662642488:RT=1662711196:S=ALNI_MZla9XgQz7IbqUmS19Bo8wt7aiVvQ",}
headers['Referer']=link
resp    = session.get(link,headers=headers)
cookies = requests.utils.cookiejar_from_dict(requests.utils.dict_from_cookiejar(session.cookies))
headers["Content-Type"] = "application/x-www-form-urlencoded"
headers["Origin"] = "https://top500.org"
headers["Sec-Fetch-Mode"] = "navigate"
headers["Sec-Fetch-Site"] = "same-origin"
headers["Sec-Fetch-User"] = "?1"
headers["Upgrade-Insecure-Requests"] = "1"
headers['X-CSRFToken'] = cookies.get("csrftoken")
payload = {'csrfmiddlewaretoken':cookies.get("csrftoken") ,'login':login,'password':password}
resp    = session.post(link,headers=headers,data=payload,cookies =cookies)

if resp.ok:
  print("Connected to top500.org")
else:
  sys.exit("Connection  to top500.org Failed. ")

base_url_top500 = "https://www.top500.org/lists/top500/{}/{}/download/TOP500_"

base_url_green500 = "https://www.top500.org/files/green500/green500_top_"

def gen_dates(date_arg) -> list:
  """
  Generate Year/Month Dates From start_year to current year 
  """
  print("Generating Date")
  if len(date_arg) == 4:
    ym_list = [date_arg+"06",date_arg+"11"]
  elif len(date_arg) == 9:
    ym_list = []
    start_year = int(date_arg[:4])
    endyear = int(date_arg[-4:])
    while start_year <= endyear:
      ym_list.append(str(start_year)+"06")
      ym_list.append(str(start_year)+"11")
      start_year = start_year+1
  elif len(date_arg) == 7:
    ym_list = [date_arg.replace("-","")]

  if datetime.today().month < 11 and str(datetime.today().year) == ym_list[-1:][0][:4]:
    ym_list.pop()
  return ym_list

def gen_links(base_url: str, ym_list: list)->list:
  """
  Gen links for respective dates
  """
  links = [base_url.format(ym[:4],ym[-2:])+ym+".xls" for ym in ym_list]
  date = [str(ym[:4]+" "+ym[-2:]) for ym in ym_list]
  links_date = {"links":links,"date":date}
  return links_date


def upload(data:pd.DataFrame):
  df = data
  print("uploading top500")
  i = 0
  while i < len(data):
    print("uploading {}".format(i))
    body = {}
    for col in df.columns:
      if str(df[col].dtype) == "object":
        if df[col][i] == -9999 or str(df[col][i]) == str(-9999):
          body[col.lower().replace(" ","_")] = None
        else:
          body[col.lower().replace(" ","_")] = str(df[col][i])
      elif col == "doc_date" :
        body[col.lower().replace(" ","_")] = df[col][i]
      elif col.lower() == "year":
        if df[col][i] != -9999:
          body["installation_year"] = datetime.strptime(str(df[col][i])+"02","%Y%m")
        else:
          body["installation_year"] = None
      else:
        if df[col][i] == -9999 or str(df[col][i])== str(-9999):
         body[col.lower().replace(" ","_")] = None 
        else:      
          body[col.lower().replace(" ","_")] = float(df[col][i])

    body_ = {}
    for key in body.keys():
      if key != "effeciency_(%)" and key not in body_.keys():
        body_[key] = body[key]
    #body_ = json.loads(json.dumps(body).replace("-9999",'null'))
    body_["load_date"] = datetime.today()

    es_client.index(
        index="tdap_hpc_top500",
        id = "".join(chr for chr in str(df["Computer"][i]) if chr.isupper())  + str(i),
        body= body_,
        
    )
    i=i+1
  print("{} docs uploaded".format(i))

def upload_green(data:pd.DataFrame):
  df = data
  print("uploading green500 ")
  i = 0
  while i < len(df):
    print("uploading {}".format(i))
    body = {}
    for col in df.columns:
      if str(df[col].dtype) == "object":
        if df[col][i] == -9999 or str(df[col][i])== str(-9999):
          body[col.lower().replace(" ","_").replace("[","_").replace("]","_").replace("(","_").replace(")","_").replace(".","_").replace("/","_")] = None
        else:
          body[col.lower().replace(" ","_").replace("[","_").replace("]","_").replace("(","_").replace(")","_").replace(".","_").replace("/","_")] = str(df[col][i])
      elif col == "doc_date" :        
        body[col.lower().replace(" ","_")] = df[col][i]
      elif col.lower() == "year":
        if df[col][i] != -9999:
          body["installation_year"] = datetime.strptime(str(df[col][i])+"02","%Y%m")
        else:
          body["installation_year"] = None        
      else:        
        if df[col][i] == -9999 or str(df[col][i])== str(-9999):
          body[col.lower().replace(" ","_").replace("[","_").replace("]","_").replace("(","_").replace(")","_").replace(".","_").replace("/","_")] = None
        else:
          body[col.lower().replace(" ","_").replace("[","_").replace("]","_").replace("(","_").replace(")","_").replace(".","_").replace("/","_")] = float(df[col][i])
    body["load_date"] = datetime.today()
    # print(body)
    
    body_ = {}
    for key in body.keys():
      if key != "effeciency_(%)" and key not in body_.keys():
        body_[key] = body[key]
    es_client.index(
        index="tdap_hpc_green500",
       # id = data["Computer"]+data["doc_date"],
        id = "".join(chr for chr in str(df["Computer"][i]) if chr.isupper()) +str(body_["doc_date"]),
        body = body_,
        
    )
    i = i +1

if args["date"].lower() == "all":
  top500_links = gen_links(base_url_top500,gen_dates("1993-"+str(datetime.today().year)))

  green500_links = gen_links(base_url_green500,gen_dates("2013-"+str(datetime.today().year)))
else:
  top500_links = gen_links(base_url_top500,gen_dates(date_arg))

  green500_links = gen_links(base_url_green500,gen_dates(date_arg))


#Handling Fetching Of Top500
top500_dfs = [] 
i = 0
while i < len(top500_links["links"]):
  x = top500_links["links"][i]
  data = session.get(x)
  if str(404) in str(data):
    data = session.get(x+"x")
    df = pd.read_excel(data.content)
    df = df.fillna(-9999)
    date = top500_links["date"][i] + " 03"
    print(date)
    df["doc_date"] =((date+'-')*(len(df)-1)).split('-') 
    df= df.reset_index(drop=True)
  else:
    df = pd.read_excel(data.content)
    if "Unnamed: 1" in df.columns:
      df.columns = df[:1].iloc[0]
      df = df.drop(labels=0,axis=0)
    df = df.fillna(-9999)
    date = top500_links["date"][i] + " 03"
    print(date)
    df["doc_date"] =((date+'-')*(len(df)-1)).split('-')
    df= df.reset_index(drop=True)
  top500_dfs.append(df)
  i = i +1
dff = pd.concat(top500_dfs,axis=0,join='outer')
dff = dff.reset_index(drop=True)
dff = dff.fillna(-9999)
print(dff["Computer"][499])
dff["doc_date"] = pd.to_datetime(dff["doc_date"])
print("NaT" in dff["doc_date"])
print(str(dff["doc_date"][499]))

i=0
while i < len(dff["doc_date"]):
  if str(dff["doc_date"][i]) == "NaT":
    dff["doc_date"][i] = dff["doc_date"][i-1]
  i = i +1
print(dff["doc_date"][499])


upload(dff)

#Handling Fetching Of green500
dfs500 = []
i=0
while i < len(green500_links["links"]):
  x = green500_links["links"][i]
  data = session.get(x)
  if str(404) in str(data):
    data = session.get(x+"x")
    df = pd.read_excel(data.content)
    df = df.fillna(-9999)
    date = green500_links["date"][i] + " 03"
    df["doc_date"] =((date+'-')*(len(df)-1)).split('-')
    df.index = range(0,len(df))
  else:
    df = pd.read_excel(data.content)
    if "Unnamed: 1" in df.columns:
      df.columns = df[:1].iloc[0]
      df = df.drop(labels=0,axis=0)
    df = df.fillna(-9999)
    date = green500_links["date"][i] +" 03"
    print(date)
    df["doc_date"] =((date+"-")*(len(df)-1)).split('-')
    df.index = range(0,len(df))
  dfs500.append(df)
  i = i +1
#outer join on dataframes 
dff500 = pd.concat(dfs500,axis=0,join='outer')
dff500.index = range(0,len(dff500))
dff500 = dff500.fillna(-9999)
dff500["doc_date"] = pd.to_datetime(dff500["doc_date"])
i=0
while i < len(dff500["doc_date"]):
  if str(dff500["doc_date"][i]) == "NaT":
    dff500["doc_date"][i] = dff500["doc_date"][i-1]
  i = i +1
dff500.to_csv("green500.csv",index=False)
upload_green(data=dff500)

sys.exit("Done")

# python top500_ingestion.py -es Prod -d all