from ast import keyword
from operator import index
from kibana_api import Kibana
import sys
import json
from os.path import exists
import configparser
from time import time
from argparse import ArgumentParser

argparser = ArgumentParser()

argparser.add_argument("-cfg","--Config",help="Specify Config File",default='es_env.cfg')
argparser.add_argument("-obj","--Object",help="Specify Saved objects to copy  index-patterns or dasboards")
argparser.add_argument("-org","--Origin-space",help="Specify Source Space to copy the saved object from")
argparser.add_argument("-ds","--Destination-space",help="Specify Source Space to copy the saved object from")

args = argparser.parse_args()
start = time()
ES_Env = configparser.ConfigParser()
print(args.Config)
if not exists(args.Config):
    sys.exit("Config File Missing!")
else:
    ES_Env.read(args.Config)
if str(args.Object) == "index-patterns" :
    object_type = str(args.Object)
elif str(args.Object) == "dashboards":
    object_type = str(args.Object)
else:
    sys.exit("Specify Correct Object Type  index-patterns or dashboards")
#parsing config file

URL = ES_Env["AUTH_CREDS"]["URL"]
USERNAME = ES_Env["AUTH_CREDS"]["USERNAME"]
PASSWORD = ES_Env["AUTH_CREDS"]["PASSWORD"]

#Handling Connection to Elasticsearch
kibana = Kibana(base_url=URL, username=USERNAME, password=PASSWORD)
if kibana.ping:
    print("Connected To Elasticsearch")
else:
    sys.exit("Failed to Connect to Elasticsearch")

#fetch all space objects present on kibana 
spaces_objects = kibana.space().all().json()

spaces = []
for space_obj in spaces_objects:
    spaces.append(space_obj["name"].lower().replace(" ","-"))
#handling space inputs
Origin = args.Origin_space.lower()
Destination = args.Destination_space.lower()


if Origin not in spaces:
    sys.exit("Origin space {} not valid \n NOTE replace space with - if space exist within spacename \n (Example data-scientist not data scientist) ".format(Origin)) 
elif Destination not in spaces:
    sys.exit("Destination space {} not valid \n NOTE: replace space with - if space exist within spacename \n (Example data-scientist not data scientist) ".format(Destination)) 
else:
    print("Specified Spaces are Valid \n Proceeding...")




def copy_index_pattern(space,destination_space):
    """
    function to copy index pattern
    space - the space that contains the needed index-patterns (source space)
    destination_space - the destination space
    """
    response_obj= kibana.object(space_id=space).all(type="index-pattern")
    response_obj_ds= kibana.object(space_id=destination_space).all(type="index-pattern").json()
    if response_obj.ok:
        response_json = response_obj.json()
        index_patterns = response_json["saved_objects"]
        index_patterns_ds = response_obj_ds["saved_objects"]
        print(len(index_patterns))
        index_patterns_org = [{"id":x["id"],"title":x["attributes"]["title"]} for x in index_patterns if x["attributes"]["title"][:4]=="tdap"]
        index_patterns_ds = [{"id":x["id"],"title":x["attributes"]["title"]} for x in index_patterns_ds]
        new = [x for x in index_patterns_org if x not in index_patterns_ds]
        if len(new) == 0:
            print("No new index-patterns on {} space".format(space))
        else:
            print("new index-patterns detected \n pushing new index-patterns")
            for index_pattern in index_patterns:

               
                i = 0
                while i < len(new):
                    if new[i]["id"] == index_pattern["id"]:             
                        push_response = kibana.object(space_id=destination_space).create(id=index_pattern["id"],type="index-pattern",attribs=index_pattern["attributes"])
                        if push_response.ok:
                            print("copied {} to {} space successfuly ".format(index_pattern["attributes"]["title"],destination_space))     
                        else:
                            print("failed to copy {}".format(index_pattern["attributes"]["title"])) 
                    i = i +1
    print("All index-patterns copied to {}".format(destination_space)) 

def copy_dashboard(space,destination_space):
    """
    function to copy dashboards
    space - the space that contains the needed dashboards (source space)
    destination_space - the destination space
    """
    response_obj= kibana.object(space_id=space).all(type="dashboard")
    if response_obj.ok:
        response_json = response_obj.json()

        dashboards = response_json["saved_objects"]
        print(len(dashboards))
        for dashboard in dashboards:
                exist = kibana.object(space_id=destination_space).get(id=dashboard["id"],type="dashboard")
                if exist.ok:
                    print(exist.json().keys())
                # sys.exit("done")
                    print("{}  exists in destination space \n skipping".format(exist.json()["attributes"]["title"]))
                
                else:                   
                    push_response = kibana.object(space_id=destination_space).create(id=dashboard["id"],type="dashboard",attribs=dashboard["attributes"])
                    if push_response.ok:
                        print("copied {} to {} space successfuly ".format(dashboard["attributes"]["title"],destination_space))      
    print("All index-patterns copied to {}".format(destination_space)) 



if object_type == "index-patterns":
    copy_index_pattern(space=Origin,destination_space=Destination)
else:
    copy_dashboard(space=Origin,destination_space=Destination)
finish = time()
execution_time = finish - start
print("Execution time is {} seconds".format((execution_time//1)))
sys.exit("Done")
