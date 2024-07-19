from rich.pretty import pprint
import requests
import csv
import json

base_uri = "https://adb-2627578047106893.13.azuredatabricks.net"
token = ""

def job_get(job_id):

    endpoint = '/api/2.1/jobs/get'
    headers = {'Authorization': f'Bearer {token}'}
    params = {
        "job_id":job_id
    }

    response = requests.get(base_uri + endpoint, headers=headers, params=params)
    response_json = response.json()
    pprint(response_json)
    return response_json

def instance_pools_list():

    endpoint = '/api/2.0/instance-pools/list'
    headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json'}
    params = {
        
    }

    response = requests.get(base_uri + endpoint, headers=headers, params=params)
    response_json = response.json()
    pprint(response_json)

def list_clusters():

    endpoint = "/api/2.1/clusters/list"
    headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json'}
    params = {
        
    }

    response = requests.get(base_uri + endpoint, headers=headers, params=params)
    response_json = response.json()
    pprint(response_json)

def modificar_procesos():
    endpoint = '/api/2.1/jobs/update'
    headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json'}

    with open('input/workflows.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        reader.__next__()
        for row in reader:
            job_config = job_get(row[0])
            job_config_settings = job_config.get("settings",{})
            job_clusters_config = job_config_settings.get("job_clusters")

            for idx, job_cluster_settings in enumerate(job_clusters_config, start=0):
                job_clusters_config[idx]["new_cluster"]["autoscale"]["min_workers"] = row[1]
                job_clusters_config[idx]["new_cluster"]["autoscale"]["max_workers"] = row[2]              
                
            params = {
                "job_id": job_config.get("job_id"),
                "new_settings":{
                    "job_clusters":job_clusters_config
                }
            }
                        
            pprint(params)
            params = json.dumps(params)
            
            response = requests.post(base_uri + endpoint, headers=headers, data=params)
            response_json = response.json()
            pprint(response_json)


#job_get(373718931608645)
#instance_pools_list()
modificar_procesos()
#list_clusters()