#change tolu on line 103 to tdap
#import required packages
import requests
import json
import requests
import certifi
from bs4 import BeautifulSoup as bs
from kibana_api import Kibana ,Visualization ,Panel,Dashboard
import os
import sys
import pandas as pd
os.environ["http_proxy"]= "http://proxy-chain.intel.com:911"

os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"

py_cacert =  certifi.where() # get the location of default python ssl3 certificate
with open('C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/json bulk dashboard/cacerts.pem', 'rb') as infile: 
    #open and read in the custom ssl3 certificate file into a variable
    tdap_cacert = infile.read()
with open(py_cacert,'ab') as outfile:
    #appending the custom certificate to  python default
    outfile.write(tdap_cacert)

URL = "https://eagles-clx15.jf.intel.com:5601/" # kibana host

#Dev credentials
USERNAME = "elastic" 
PASSWORD = "tdaptdap"


kibana = Kibana(base_url=URL, username=USERNAME, password=PASSWORD) #connect to kibana api
if kibana.ping:
    print("Connected")
else:
    print("Failed To Connect To Elasticsearch")

#get visualization json format from kibana api

dashboard_requirements = pd.read_csv("C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/json bulk dashboard/dashboard update.csv")
id = "a26d1e70-29d0-11ed-9306-3d122ea54542"

#Remove missing Values From Dashboard Requirements
dashboard_requirements.dropna(inplace=True)


#List of dictionaries that contains {filter:Display name} where filter = github repo name
filter_list  = []

i = 0
while i < len(dashboard_requirements):
    filter_list.append({"filter":dashboard_requirements["GitHub Repo name"][i],"display_name":dashboard_requirements["Name for Display"][i]})
    i =i+1

id = "a26d1e70-29d0-11ed-9306-3d122ea54542" #id for template dashboard (Clear Linux)

def create_dash(filter_,dashboard_template_json):
    """
    To create dashboard for each filter/reponame
    filter_ ==> the keyword to be filter_ed by --> string
    dashboard_template_json ==> Existing dashboard template --> Json
    """
    new_dashboard = {} # new variable to contain the json for the new dashboard
    new_dashboard["attributes"] = dashboard_template_json["attributes"] 
    new_dashboard["references"] = dashboard_template_json["references"]
    new_dashboard["attributes"]["title"] = "Community Dashboard - {}".format(filter_["display_name"])
    panels = []
    #iterating through each panels in the template dashboard and replacing the filters and display names in markdown visualizations accordingly
    for panel_ in json.loads(dashboard_template_json["attributes"]["panelsJSON"]):
        if "attributes" in panel_["embeddableConfig"].keys():
            panel_["embeddableConfig"]["attributes"]["state"] = json.loads(json.dumps(panel_["embeddableConfig"]["attributes"]["state"]).replace("clearlinux",filter_["filter"]))
            panel_["embeddableConfig"]["attributes"]["state"] = json.loads(json.dumps(panel_["embeddableConfig"]["attributes"]["state"]).replace("kubectl",filter_["filter"]))
            panel_["embeddableConfig"]["attributes"]["state"] = json.loads(json.dumps(panel_["embeddableConfig"]["attributes"]["state"]).replace("config.load_kube_config",filter_["filter"]))
            panels.append(panel_)
        elif "savedVis" in  panel_["embeddableConfig"].keys():
                panel_["embeddableConfig"]["savedVis"]["params"]["markdown"] =  panel_["embeddableConfig"]["savedVis"]["params"]["markdown"].replace("Clear Linux",filter_["display_name"])
                panel_["embeddableConfig"]["savedVis"]["params"]["markdown"] =  panel_["embeddableConfig"]["savedVis"]["params"]["markdown"].replace("Clear Linux",filter_["display_name"])
                panel_["embeddableConfig"]["savedVis"]["params"]["markdown"] =  panel_["embeddableConfig"]["savedVis"]["params"]["markdown"].replace("CESG","Brockmeier, Joe")
                panels.append(panel_)
    for panel in panels:
        if "attributes" in panel["embeddableConfig"].keys():
            if "Clear Linux".lower() in panel["title"].lower():
                panel["title"] = panel["title"].replace("Clear Linux",filter_["display_name"])
            elif "Clear Linux".lower() not in panel["title"].lower() and filter_["display_name"].lower() not in panel["title"].lower() :
                panel["title"] = filter_["display_name"] +"-" +str(panel["title"])
            elif filter_["filter"].lower() in panel["title"].lower():
                panel["title"] = panel["title"].replace(filter_["filter"],filter_["display_name"])
            
    new_dashboard["attributes"]["panelsJSON"]  = json.dumps(panels)
    new_dashboard  = json.loads(json.dumps(new_dashboard).replace("community_","code_"))
    print("Creating {} Dashboard".format(filter_["filter"].capitalize()))
    dashboard_response = kibana.object(space_id="tolu").create(id="community_"+filter_["filter"].lower().replace(" ",""),type='dashboard', body=new_dashboard)
    #handling dashboard esponse
    if dashboard_response.ok:
        response={}
        response["base_url"]=[URL+"s/tdap/app/dashboards#/view/"+"community_"+filter_["filter"].lower().replace(" ","")]
        response["status"] =["Successfully Created"]
        response["title"] = ["Community Dashboard - {}".format(filter_["filter"])]
        return response
    else: 
        response={}
        response["base_url"]=["NA"]
        response["status"] =["Failed"]
        response["title"] = ["Community Dashboard - {}".format(filter_["filter"])]
        return response

#iterating through the list of filter_list
df = []
i = 0
while i < len(filter_list):
    reply = create_dash(filter_=filter_list[i],dashboard_template_json=kibana.object(space_id="tdap").get(id="community_clearlinux",type="dashboard").json())
    df.append(pd.DataFrame(reply))  
    i = i+1

#generating report
df = pd.concat(df,axis=0)
df.index = range(0,len(df))
df.to_csv("report.csv",index=False)
