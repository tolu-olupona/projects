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
# argparser.add_argument("-org","--Origin-space",help="Specify Source Space to copy the saved object from")
argparser.add_argument("-ds","--Destination-space",help="Specify Source Space to copy the saved object from")

args = argparser.parse_args()
start = time()
ES_Env = configparser.ConfigParser()
if not exists(args.Config):
    sys.exit("Config File Missing!")
else:
    ES_Env.read(args.Config)

if str(args.Object) == "index-patterns" :
    object_type = "index-pattern"
elif str(args.Object) == "dashboards":
    object_type = "dashboard"
else:
    sys.exit("Specify Correct Object Type  index-patterns or dashboards")
# if str(args.Object) == "index-patterns" :
#     object_type = str(args.Object)
# elif str(args.Object) == "dashboards":
#     object_type = str(args.Object)
# else:
#     sys.exit("Specify Correct Object Type  index-patterns or dashboards")
#parsing config file

URL = ES_Env["AUTH_CREDS"]["URL"]
USERNAME = ES_Env["AUTH_CREDS"]["USERNAME"]
PASSWORD = ES_Env["AUTH_CREDS"]["PASSWORD"]

# command = args.Command
# if command.lower() == "copy" :
#     print("Command {} valid proceeding...".format(command))
# elif command.lower() == "delete":
#     print("Command {} valid proceeding...".format(command))
# else:
#     sys.exit("Command {} invalid".format(command))

#Handling Connection to Elasticsearch
kibana = Kibana(base_url=URL, username=USERNAME, password=PASSWORD)
if kibana.ping:
    print("Connected To Elasticsearch")
else:
    sys.exit("Failed to Connect to Elasticsearch")

spaces_objects = kibana.space().all().json()
spaces = []
for space_obj in spaces_objects:
    spaces.append(space_obj["name"].lower().replace(" ","-"))
#handling space inputs

Destination = args.Destination_space.lower()


if Destination not in spaces:
    sys.exit("Destination space {} not valid \n NOTE: replace space with - if space exist within spacename \n (Example data-scientist not data scientist) ".format(Destination)) 
elif Destination == "tdap" :
    sys.exit("can't delete on tdap oops!")
else:
    print("Specified Spaces are Valid \n Proceeding...")

def delete_saved_object(destination_space,object_type):

    response_obj= kibana.object(space_id=destination_space).all(type=object_type)
    if response_obj.ok:
        response_json = response_obj.json()
        index_patterns = response_json["saved_objects"]
        for x in index_patterns:
           kibana.object(space_id=destination_space).delete(id = x["id"],type=object_type)
    response_obj= kibana.object(space_id=destination_space).all(type=object_type)
    print(response_obj.json())
    if response_obj.json()["total"] == 0:
        print("deleted all {}s on {} space".format(object_type,destination_space))
        sys.exit("Done")
    else:
        print("Delete Failed")

        sys.exit("Done")

                # exist = kibana.object(space_id=destination_space).get(id=dashboard["id"],type="dashboard")
                # if exist.ok:
                #     print(exist.json().keys())
                # # sys.exit("done")
                #     print("{}  exists in destination space \n skipping".format(exist.json()["attributes"]["title"]))
                
                # else:                   
                #     push_response = kibana.object(space_id=destination_space).create(id=dashboard["id"],type="dashboard",attribs=dashboard["attributes"])
                #     if push_response.ok:
                #         print("copied {} to {} space successfuly ".format(dashboard["attributes"]["title"],destination_space))      

delete_saved_object(destination_space=Destination,object_type=object_type)