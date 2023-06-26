#import required libraries
from ast import keyword
from operator import mod
from kibana_api import Kibana
import sys
import json
import configparser
import pandas as pd
import argparse
import os
import sys
import pandas as pd
import certifi


os.environ["http_proxy"]= "http://proxy-chain.intel.com:911"

os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"


# parsing config file
ES_Env = configparser.ConfigParser()
ES_Env.read("C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/Dashboards Automation/kibana-api-main/kibana-api-main/es_env.cfg")

URL = ES_Env["AUTH_CREDS"]["URL"] #kibana host
USERNAME = ES_Env["AUTH_CREDS"]["USERNAME"]
PASSWORD = ES_Env["AUTH_CREDS"]["PASSWORD"]





#Handling Connection to Elasticsearch
kibana = Kibana(base_url=URL, username=USERNAME, password=PASSWORD)
if kibana.ping:
    print("Connected To Elasticsearch")
else:
    sys.exit("Failed to Connect to Elasticsearch")

#handling 
response_obj= kibana.object(space_id="tolu").get(type="dashboard",id="4b0ba330-192d-11ed-87d5-39c345f7d100")

json_obj  = response_obj.json()
def get_tag(name, space):
    """
    Get tag by name
    name -> tag name 
    space -> space where the tag is saved
    """
    tags_response = kibana.object(space_id=space).all(type="tag")
    tags_list = tags_response.json()["saved_objects"]
    for tag in tags_list:
        if tag["attributes"]["name"].lower() == name.lower():
            tag_obj = {
                "name": "tag-"+tag["id"],
                "id": tag["id"],
                "type":"tag"
            }
        else: 
            tag_obj = None
    if tag_obj is not None:
        return tag_obj
    else:
        print("tag {} not found or does not exist".format(name))
        tag_obj = "tag not found"
        return tag_obj

def get_header(space,dashboard_id):
    """
    Get the markdown visualization template from a specified dashboard
    space -> space where the dashboard object is saved
    dashboard_id -> id of the dashboard that contains the markdown visualization template
    """
    dashboard_response = kibana.object(space_id=space).get(type="dashboard",id=dashboard_id)
    dashboard_json= dashboard_response.json()
    for panel in json.loads(dashboard_json["attributes"]["panelsJSON"]):
        if "tdap_owner" in str(panel):
            header = panel
            break
    return header

def get_dashboard_obj(space,title):
    """
    Get dashboard object by title
    space -> space where the dashboard is saved
    title -> dashboard title
    """
    dashboards_response = kibana.object(space_id =space).find(type="dashboard",search=title)
    needed_dashboard = None
    for dashboard in dashboards_response.json()["saved_objects"]:
        if title.replace("\n","").replace("\t","") in str(dashboard):
            print("found")
            needed_dashboard = dashboard
            break
    if needed_dashboard is not None:
        return needed_dashboard
    else:
        print("{} not found check the title and try again".format(title))
        return "Dashboard does not exist"




dashboard_requirements = pd.read_csv("C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/Dashboards Automation/Dashboard Cleanup List.csv")
dashboard_requirements.fillna("None",inplace=True)

#List of dictionaries that contains {filter:Display name} where filter = github repo name
iter_list  = []

i = 0
while i < len(dashboard_requirements):
    iter_list.append({"title":dashboard_requirements["Dashboard Title"][i],
    "header":dashboard_requirements["Header"][i],
    "biz_contact":dashboard_requirements["Biz Contact"][i],
    "tdap_owner":dashboard_requirements["TDAP Owner"][i],
    "other_contributors":dashboard_requirements["Other Contributors"][i],
    "descr":dashboard_requirements["Description"][i],
    "tags":dashboard_requirements["Tags"][i],
    })
    i = i+1
#handling dashboard modification
def edit_dashboard_json(tags,header, descr, needed_dashboard):
    """
    header -> markdown visualization object
    tags -> list of tag object(s)
    needed_dashboard -> the json of a dashboard in iteration
    descr -> description text for the dashboard that needs updating
    """
    if type(needed_dashboard) != str:
    
        if header != "None":
            
            mod_js = json.loads(needed_dashboard["attributes"]["panelsJSON"]) #modified panelsJSON
            mod_js.insert(0,header) #insert the required viz i.e markdown
            needed_dashboard["attributes"]["panelsJSON"]  = json.dumps(mod_js)
                                                                                                                                                                                                                                                                                                                                                                                                        
        if "description" in needed_dashboard["attributes"].keys():
                needed_dashboard["attributes"]["description"] = descr
        else:
            print(needed_dashboard["attributes"])
        if tags is not None:
            for tag in tags:
                needed_dashboard["references"].append(tag)
        dashboard_response = kibana.object(space_id="tolu").update(id=needed_dashboard["id"],type='dashboard', attribs=needed_dashboard["attributes"],references = needed_dashboard["references"])  #json response from
        print("{} Updated".format(needed_dashboard["attributes"]["title"]))
        return dashboard_response
    else:
        print(needed_dashboard)
#ITERATING THROUGH THE REQUIREMENTS LIST 
for iter in iter_list:
    if str(iter["header"]) == "True":
        header = get_header(space="tolu",dashboard_id="4b0ba330-192d-11ed-87d5-39c345f7d100")
        header = json.dumps(header)
        header = header.replace("biz_contact",str(iter["biz_contact"]))
        header = header.replace("tdap_owner",str(iter["tdap_owner"]))
        if iter["other_contributors"] != "None":           
            header = header.replace("other_contributors",str(iter["other_contributors"]))
        else:
            header = header.replace("*Other Contributors* : other_contributors","")
        header = json.loads(header,strict=False)

           
    else:
        header =  "None"
    if iter["tags"] != "None":
        if "," in iter["tags"]:
            Tags_list = iter["tags"].split(",") 
            tags_obj = []
            for tag in Tags_list:
                tags_obj.append(get_tag(name=tag,space="tolu"))
        else:
            tags_obj = [get_tag(name=iter["tags"],space="tolu")]
    else:
        tags_obj = None

    edit_dashboard_json(tags=tags_obj,header=header,descr=iter["descr"],needed_dashboard=get_dashboard_obj(space="tolu",title=iter["title"]))
    
sys.exit("Done")


