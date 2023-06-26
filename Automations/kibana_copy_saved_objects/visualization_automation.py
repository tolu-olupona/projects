from ast import keyword
from kibana_api import Kibana, Panel , Visualization, Dashboard
import sys
import json
import configparser

import argparse
import pandas as pd

parser = argparse.ArgumentParser()
#handling required arguments
parser.add_argument("-cfg", "--Config", help = "specify config file" )

#parsing config file
ES_Env = configparser.ConfigParser()
ES_Env.read('C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/Dashboards Automation/kibana-api-main/kibana-api-main/es_env.cfg')
URL = ES_Env["AUTH_CREDS"]["URL"]
USERNAME = ES_Env["AUTH_CREDS"]["USERNAME"]
PASSWORD = ES_Env["AUTH_CREDS"]["PASSWORD"]

#Handling Connection to Elasticsearch
kibana = Kibana(base_url=URL, username=USERNAME, password=PASSWORD)
if kibana.ping:
    print("Connected To Elasticsearch")
else:
    sys.exit("Failed to Connect to Elasticsearch")
csv = pd.read_csv("C:/Users/tolupona/OneDrive - Intel Corporation/Desktop/Dashboards/Dashboards Automation/sustainability_viz_panel_titles.csv")


#filters = ["Chromium","AI or Artificial Intelligence","Kubernetes","Kubectl","Sdk"]
filters = [x for x in csv["filters"]]

viz_s = []
for filter_ in filters:
    viz=kibana.object(space_id="tolu").get(type="lens",id="00ec6290-6c3d-11ed-9435-23a638dab09a")
    viz = viz.json()
    viz["attributes"]["state"]["query"]["query"] = filter_
    viz["title"] = filter_
    viz["attributes"]["title"] = filter_

    visualization_response = kibana.object(space_id="tolu").create_(type='lens', attribs=viz["attributes"],references=viz["references"])
    viz_s.append(visualization_response.json())
print(len(viz_s))

dash=kibana.object(space_id="tolu").get(type="dashboard",id="ed039d90-6c3f-11ed-9435-23a638dab09a")
md = [x for x in range(0,((len(filters)//4)+1))]

w=12
h=8
j = 0
grids = []
while j < len(md):
    i=0
    while i < 4:
        grids.append({"x":w*i,"y":h*md[j],'w':w,'h':h})
        i = i+1
    j = j +1
panels = []
references = []
count = 0
i = 0
print(len(md))
print(len(grids))
print(len(viz_s))
grids = grids[:len(viz_s)]
print(len(grids))

while i < len(grids):
    count+=1
    panel = Panel("panel_{}".format(count), grids[i]['w'], grids[i]['h'], grids[i]['x'], grids[i]['y'], visualization_id=viz_s[i]["id"])
    apanel = panel.create()
    panels.append(apanel)
    references.append(panel.get_reference())
    i = i+1
dashp = json.loads(dash.json()["attributes"]["panelsJSON"])
for p in panels:
    dashp.append(p)

f = dash.json()
f["attributes"]["panelsJSON"] = json.dumps(dashp)
for r in references:
    f["references"].append(r)

response = kibana.object(space_id="tolu").update(id="ed039d90-6c3f-11ed-9435-23a638dab09a",type="dashboard",attribs=f["attributes"],references=f["references"])
print(response.json())
sys.exit("Done")

panelv["panelIndex"] = "nnnew"
v2 = visualization_response.json()
v2["attributes"]["state"]["query"]["query"] = "CRS-OS"
v2["title"] = "CRS"
panelv["embeddableConfig"] = v2
dashp.append(apanel)
f = dash.json()
f["attributes"]["panelsJSON"] = json.dumps(dashp)
f["references"].append(panel.get_reference())
response = kibana.object(space_id="default").update(id="781320e0-6326-11ed-ab9e-098a41949484",type="dashboard",attribs=f["attributes"],references=f["references"])
print(response.json())
sys.exit("Done")
visualization = Visualization(type=type, title=title, index_pattern_id="e23df67f-85ce-42c1-a25b-3f62341a5942").create(query="IRIX") # this operation returns a JSON body not a request response
visualization_response = kibana.object(space_id="default").create(type='visualization', body=visualization)
visualization_json = visualization_response.json()
visualization_id = visualization_json["id"]

panel = Panel("panel_0", 48, 12, 0, 2, visualization_id=visualization_id)
panels = [panel.create()]
references = [panel.get_reference()]
dashboard = Dashboard(title="Demo Dashboard", panels=panels, references=references).create() # this operation returns a JSON body not a request response

dashboard_response = kibana.object(space_id="default").create('dashboard', body=dashboard)
dashboard_json = dashboard_response.json()
print(dashboard_json)
sys.exit("Done")


#handling 
response_obj= kibana.object(space_id="default").get(type="dashboard",id="f490cbd0-6188-11ed-ab9e-098a41949484")
lens_obj  = response_obj.json()
panels = json.loads(lens_obj["attributes"]["panelsJSON"])

w = Visualization(type="line", index_pattern_id="e23df67f-85ce-42c1-a25b-3f62341a5942",title="try").create()




filters = ["CNL","Calibre OS","CRS-OS","IRIX"]
grids = []
i=0
x_count=1
y_count=0
md = [x for x in range(0,((len(filters)%3)+1))]
w=16
h=8
j = 0
while j < len(md):
    if j==0:
        i = 1
    else:
        i=0
    while i < 3:
        grids.append({"x":w*i,"y":h*md[j],'w':w,'h':h})
        i = i+1
    j = j +1
print(grids)


 # this operation returns a JSON body not a request response
i = 0
while i < len(filters):
    viz_res = kibana.object(space_id="default").get(id="979c9060-62bb-11ed-ab9e-098a41949484",type="lens") 
    viz_obj = viz_res.json()
    viz_obj["attributes"]["state"]["query"]["query"] = filters[i]
    viz_obj["attributes"]["title"]= filters[i]
    viz_obj["id"] = filters[i].replace(" ","").replace("-","")
    viz_res_ = kibana.object(space_id="default").create(type='lens',id=viz_obj["id"], body=viz_obj)
    print(viz_res_)
    sys.exit("done")
    grid = grids[i]
    grids[i]["i"] =  filters[i].replace(" ","")
    panel_i["gridData"] = grid
    panel_i["panelIndex"] = filters[i].replace(" ","")
    panel_i["embeddableConfig"]["attributes"]["state"]["query"]["query"] = filters[i]
    panels.append(panel_i)
    i = i+1
for panel in panels:
    print(panel["gridData"])


lens_obj["attributes"]["panelsJSON"] = json.dumps(panels)
response = kibana.object(space_id="default").update(id="f490cbd0-6188-11ed-ab9e-098a41949484",type="dashboard",attribs=lens_obj["attributes"])
print(response)
sys.exit("Done")

new_viz = json.loads(lens_obj["attributes"]["panelsJSON"])[-1]

print(new_viz)

sys.exit("DOne")
req_dash = kibana.object(space_id="default").get(type="dashboard",id="calibreos").json()
newjs = json.loads(req_dash["attributes"]["panelsJSON"])
newjs.insert(0,new_viz)
req_dash["attributes"]["panelsJSON"] = json.dumps(newjs)
print(req_dash["attributes"])
dashboard_response = kibana.object(space_id="default").update(id="calibreos",type='dashboard', attribs=req_dash["attributes"])  #json response from
print(dashboard_response.ok)
sys.exit("Done")
