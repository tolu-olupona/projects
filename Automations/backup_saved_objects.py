import requests
import os
from argparse import ArgumentParser
import time
import configparser
from os.path import exists
import json
import sys

argparser = ArgumentParser()
#handle input arguments
argparser.add_argument(
    "-cfg", "--Config", help="Specify path to  Config File", default="saved_objects_config.cfg"
)
argparser.add_argument(
    "-obj",
    "--Object",
    help="Specify Saved objects to copy  index-patterns, tags or dasboards",
)
argparser.add_argument(
    "-org", "--Origin-space", help="Specify Source Space to copy the saved object from"
)
argparser.add_argument(
    "-ds",
    "--Destination-space",
    help="Specify Source Space to copy the saved object from",
)
argparser.add_argument(
    "-up", "--update", help="Specify to overwrite previously created dashboards"
)



ES_Env = configparser.ConfigParser()

#paarsing arguments
args = argparser.parse_args()
if not args.Config:
    exit("Config File not Specified Exiting ....")
if not exists(args.Config):
    sys.exit("Config File Path not valid Exiting....!")
else:
    try:
        ES_Env.read(args.Config)
        BASE_URL = ES_Env["URL"]["BASE_URL"]
        api_key = ES_Env["AUTH"]["API_KEY"]
        print("Config File Valid")
    except Exception as error:
        print("Invalid config file"+ str(error))
        exit()
#handling svaed objects type
if str(args.Object).lower() in ["index-patterns", "tags", "dashboards", "all","visualizations","lens","maps"]:
    object_type = str(args.Object).lower()
    print("Specified Object Valid")
else:
    sys.exit(
        'Specify Correct Object type should be one of "index-patterns","tags","dashboards","all" '
    )
if not args.Origin_space:
    print("Specify origin space")
    exit()
if not args.Destination_space:
    print("Specify destination  space")
    exit()

base_url = f"{BASE_URL}"
all_spaces_url = base_url + "/api/spaces/space" #all spaces on kibana url
headers = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {api_key}",
}


def ping_kibana(url):
    """
    Check if kibana is active
    """
    try:
        status = requests.get(url)
        print(status.status_code)
        if status.status_code != 500 or status.status_code in [200, 201, 401, 404]:
            return True
        else:
            return False
    except Exception as e:
        return False


if ping_kibana(base_url):
    print("Kibana is active")
else:
    print("kibana is inactive exiting ...")
    exit()


all_spaces = requests.get(all_spaces_url, headers=headers).json()
all_spaces = [space["id"] for space in all_spaces]
origin = args.Origin_space.lower()
destination = args.Destination_space.lower()

#validate inputted spaces
if origin not in all_spaces:
    sys.exit("Origin space not valid")

if destination not in all_spaces:
    sys.exit("Destination space not valid ")

print("Specified spaces are valid proceeding ...")

saved_objects = {
    "all": ["index-pattern", "tag", "lens", "map", "visualization"],
    "index-patterns": "index-pattern",
    "tags": "tag",
    "maps": "map",
    "lens": "lens",
    "visualizations": "visualization",
}

#specify saved_objects urls
origin_space = origin
destination_space = destination
export_url = base_url + f"/s/{origin_space}/api/saved_objects/_export"
import_url = (
    base_url + f"/s/{destination_space}/api/saved_objects/_import?overwrite=true"
)
create_url = (
    base_url + f"/s/{destination_space}/api/saved_objects/_bulk_create?overwrite=true"
)
fetch_all_url = base_url

#handling dashboard updates
#object_type - obj inputed from arguments
if object_type == "dashboards" or object_type == "all": 
    print("Fetching Origin Space dashboards") 
    #generate all dashboards url for origin space
    origin_dashboards_url = (
        base_url
        + f"/s/{origin_space}/api/saved_objects/_find?type=dashboard&per_page=1000"
    )
    #fetch all dashboards from origin space
    origin_dashboards = requests.get(url=origin_dashboards_url, headers=headers).json()[
        "saved_objects"
    ]
    print(f"{origin_space} space currently has {len(origin_dashboards)} dashboards")
    #handling update arguments
    if args.update:
        data = []
        batches = [i for i in range(0, len(origin_dashboards) + 10) if i % 5 == 0] #split dashboards in batches of 5
        i = 0
        print("Updating Dashboards in batches")
        count = 0
        #uploading batches of dashboards
        for batch in batches:
            count += 1
            print(f"updating {count} batch")
            data = []
            for dashboard in origin_dashboards[i:batch]:
                obj = {}
                obj["id"] = dashboard["id"]
                obj["type"] = "dashboard"
                obj["attributes"] = dashboard["attributes"]
                obj["references"] = dashboard["references"]
                data.append(obj)
            i = batch
            response = requests.post(create_url, headers=headers, data=json.dumps(data))
            #handle response for each batch
            if response.status_code != 200:
                print(response.json())
            else:
                print(f"batch{count} successful")
    #to copy without updating
    print("Fetching destination space dashboards")
    destination_dashboards_url = (
        base_url
        + f"/s/{destination_space}/api/saved_objects/_find?type=dashboard&per_page=1000"
    )
    destination_dashboards = requests.get(
        url=destination_dashboards_url, headers=headers
    ).json()["saved_objects"]

    print(
        f"{destination_space} space currently has {len(destination_dashboards)} dashboards"
    )
    #validate ids to resolve conflicts and check for new dashboards
    destination_dashboards_id = [
        dashboard["id"] for dashboard in destination_dashboards
    ]
    origin_dashboards_id = [dashboard["id"] for dashboard in origin_dashboards]
    # get new dashboards present in the origin space that's absent in the destination space
    new_dashboards_id = [
        id for id in origin_dashboards_id if id not in destination_dashboards_id
    ]
    print(
        f"There are currently {len(new_dashboards_id)} dashboards in {origin} space that are not in {destination} space"
    )
    if len(new_dashboards_id) == 0:
        if object_type != "all":
            print("No new dashboards found exiting...")
            exit()
        else:
            print("Proceeding")

    new_dashboards = [
        dashboard
        for dashboard in origin_dashboards
        if dashboard["id"] in new_dashboards_id
    ]
    count = 0
    data = []
    #split new dashboards into batches
    batches = [i for i in range(0, len(new_dashboards) + 10) if i % 5 == 0]
    i = 0
    count = 0
    #uploading new dashboards batches without overwriting
    for batch in batches:
        print("copying dashboards in batches")
        count += 1
        print(f"updating {count} batch")

        data = []
        for dashboard in new_dashboards[i:batch]:
            obj = {}
            obj["id"] = dashboard["id"]
            obj["type"] = "dashboard"
            obj["attributes"] = dashboard["attributes"]
            obj["references"] = dashboard["references"]
            data.append(obj)
        i = batch
        response = requests.post(create_url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print(response.json())
        else:
            print(f"batch{count} successful")
    if object_type != "all":
        print("Operation Complete Exiting ...")
        exit()
print("Dashboard update complete updating the remaining saved objects")

#handle the other object types
data = {"type": saved_objects[args.Object.lower()]}
print(f"Trying to export {args.Object.lower()} saved objects from {origin} space")
try:
    #export and save saved objects type
    response = requests.post(export_url, headers=headers, json=data)
    if response.status_code == 200:
        print("export successful")
        with open("export.ndjson", "wb") as file:
            file.write(response.text.encode("utf-8"))

    else:
        print(response.text)
        print("export failed")
        exit()
except Exception as error:
    print("An error occured"+str(error))
    exit()
# handle reading the downloaded export.ndjson file 
cwd = os.getcwd()
abs_path = os.path.abspath(cwd)
export_file = abs_path + "/export.ndjson"

print(f"Trying to import {args.Object.lower()} saved objects to {destination} space")

with open(export_file, "rb") as import_file:
    file2 = import_file.read()
#specify request body
boundary = "----WebKitFormBoundary7MA4YWxkTrZug0W"
body = (
    "--"
    + boundary
    + "\r\n"
    + 'Content-disposition: form-data; name="file"; filename="export.ndjson"\r\n'
    + "Content-Type:application/x-ndjson\r\n\r\n"
    + file2.decode()
    + "\r\n"
    + "--"
    + boundary
    + "--"
)

headers["Content-Type"] = "multipart/form-data; boundary=" + boundary
headers["Accept"] = "application/json"
response = requests.post(
    import_url, headers=headers, data=body.encode("utf-8"), timeout=2000
)
#handle import response
if response.ok:
    print(f"imported {args.Object.lower()} to destination space successfully")
    sys.exit("exiting...")
else:
    print("import failed")
    print(response.text)
exit()
