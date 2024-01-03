from scim_integrator import scim_integrator
import time 
import requests, json
import pandas as pd
import yaml




# config_path = "../Alternate Configs/config bruno.yml"
config_path = "config.yml"

if __name__ == '__main__':
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
      
    scim_runner = scim_integrator(base_config['config'],base_config['dbx_config'],base_config['groups_to_sync'] ,base_config['LOG_FILE_NAME'] ,base_config['LOG_FILE_LOCATION'])

    scim_runner.auth_aad(True)
    scim_runner.auth_aad(False)

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



 
    