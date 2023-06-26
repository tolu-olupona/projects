#DB ENGINES INGESTION SCRIPT

#import required libraries
import pandas as pd # for getting required data from source and data formatting
import argparse # for parsing arguments being passed to the script
from elasticsearch import Elasticsearch
import sys 
import configparser # for parsing the config file
import os
from datetime import date #to perform date operations
from ssl import create_default_context #for parsing the ssl cert file


#setting arguments that needs to be passed to the script
parser = argparse.ArgumentParser(description='DB engines monthly ranking update script.')
parser.add_argument("--es", help="Choose the enviroment that the data should be pushed to (Dev/Prod)",type=str, default="Dev")
#Instantiate argument variables
args = vars(parser.parse_args()) 
config = configparser.ConfigParser()
path = os.getcwd() #get path of the current working directory
config.read("db_engines_etl_es_env.cfg")
context = create_default_context(cafile=path+"\cacerts.pem")
print(context)

#Determining the Environment that the data neeeds to be pushed to
if args["es"] == "Dev":
    #Get Elastic Dev Credentials From The Config File	
	print("the data will be loaded into tdap Dev environment")
	es_host = config["Dev"]["es_host"]
	es_port = config["Dev"]["es_port"]
	es_user = config["Dev"]["es_user"]
	es_pass = config["Dev"]["es_pass"]
elif args["es"] == "Prod":
    #Get Elastic Prod Credentials From The Config File
	print("the data will be loaded into tdap Prod environment")
	es_host = config["Prod"]["es_host"]
	es_port = config["Prod"]["es_port"]
	es_user = config["Prod"]["es_user"]
	es_pass = config["Prod"]["es_pass"]
else :
	sys.exit("environment {} incorrect choose between Dev or Prod".format(args["es"]))

# configure environment proxy to allow http/https Traffic	
os.environ["http_proxy"] = config["Proxy"]["http_proxy"]
os.environ["https_proxy"] = config["Proxy"]["http_proxy"]

url = config["Source"]["url"] # get data source url from config file
source_data = pd.read_html(url) #get required source data from source
df = source_data[3][[3,4,5]][3:] #filter unrequired data from the source data

def gen_date(df):
  """generate list of date for the data collected"""
  date_= []
  i = 0 
  while i < len(df):
    date_.append(str(str(date.today())[:7]+"-01"))
    i = i+1
  return date_

#Handling Connection to Elastic Search 
def es_connector(es_host, es_port, es_user, es_pass):
    # connector to ELK as 02042021
    client = Elasticsearch(
        [es_host],
        http_auth=(es_user, es_pass),
        scheme="http",
       port=es_port,
     ssl_context=context,
        timeout=100
    )
    if client.ping():
        print("Connected to ES\n")
        return client
    else:
        sys.exit("Failed connection to ES")
        return None

#Connect to Elastic Search
es_client = es_connector(es_host, es_port, es_user, es_pass)

#rename columns
df.columns = ["database_name","database_model","score"]
df["database_name"] = df["database_name"].apply(lambda x: x.replace("Detailed vendor-provided information available",""))
df["Date"] = gen_date(df)
df["Yr Mo"] = gen_date(df)

def upload(df):
    """
    Function for uploading Data to ElasticSearch
    """
    i=0
    while i < len(df):
            es_client.index(
            index = "tdap_db_engines" , 
            body = {
            "database_name":df["database_name"][i],
            "score":df["score"][i],
            "database_model": str(df["database_model"][i]),
            "doc_date": df["Yr Mo"][i],
            #"load_date": str(date.today()),
            
            })
            print("Uploading Doc {}".format(i))
            i = i+1
    es_client.indices.refresh(index="tdap_db_engines")
df = df.reset_index(drop=True)
df = df.fillna("")


def check_exist():
    """
    Check if data exists on elastic
    """

    search = {
    "query":{ "query_string":{"query":str(gen_date(df)[0]), "default_field":"doc_date"}}
        }
    try:
        """
        run search query on elastic to check for existing date data
        """
        data = es_client.search(index="tdap_db_engines",body=search) 
        if int(data["hits"]["total"]["value"]) == 0:
            return False
        else:
            return True
    except Exception as e :
        sys.exit(e)

exists = check_exist()
if type(exists) == bool:
    """
    if data exists the script would exit 
    if it doesn't exist the data would be uploaded
    """
    if exists:
        sys.exit("Done")
    else:
        upload(df)
        sys.exit("Done")
else:
    "If there is an unforeseen error the script should exit whith appropriate error code"
    sys.exit(exists)



