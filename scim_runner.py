from scim_integrator import scim_integrator
import time 
import requests, json
import pandas as pd
import yaml




# config_path = "../Alternate Configs/config bimal.yml"
config_path =  "config.yml"

if __name__ == '__main__':
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
    with open(base_config['groups_to_sync_path']) as file:
       groups_to_sync = json.load(file)
      
    scim_runner = scim_integrator(base_config['config'],base_config['dbx_config'],groups_to_sync ,base_config['LOG_FILE_NAME'] ,base_config['LOG_FILE_LOCATION'],is_dryrun =  base_config['is_dryrun'])

    scim_runner.auth_aad(True)
    scim_runner.auth_aad(False)

    if base_config['deactivate_deleted_users']:
      begin = time.time() 
      scim_runner.deactivate_deleted_users()
      end = time.time() 
      print("Time taken to deactivate deleted users is", end-begin) 


    begin = time.time() 
    scim_runner.sync_users()
    end = time.time() 
    print("Time taken to execute the sync users is", end-begin) 

    begin = time.time() 
    scim_runner.sync_groups()
    end = time.time() 
    print("Time taken to execute the sync groups is", end-begin) 

    begin = time.time() 
    scim_runner.sync_mappings()
    end = time.time() 
    print("Time taken to execute the sync mappings is", end-begin) 

    if base_config['deactivate_orphan_users']:
      begin = time.time() 
      scim_runner.deactivate_orphan_users()
      end = time.time() 
      print("Time taken to deactivate orphan users is", end-begin) 



 
    