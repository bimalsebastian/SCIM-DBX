#!/usr/bin/env python3


from scim_integrator import scim_integrator

import yaml
import json
import os
import time
import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor,as_completed
from io import BytesIO

current_directory = os.getcwd()
print("Current working directory:", current_directory)

# config_path = "../Alternate Configs/config bimal.yml"
config_path =  '/Users/bimal.sebastian/SourceCode/BP UC Workspace ACL-Performance/configs/EAScriptPython_config.yml'



#    users_in_bpe_exclu = 

if __name__ == '__main__':
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
    with open(base_config['groups_to_sync_path']) as file:
       groups_to_sync = json.load(file)
      
    scim_runner = scim_integrator(base_config['config'],
                                  base_config['dbx_config'],
                                  groups_to_sync ,base_config['LOG_FILE_NAME'] ,
                                  base_config['LOG_FILE_LOCATION'],
                                  is_dryrun =  base_config['is_dryrun'], 
                                  Scalable_SCIM_Enabled = True, 
                                  cloud_provider ='AWS')

    # scim_runner.auth_aad(True)
    scim_runner.auth_aad(False)
    scim_runner.auth_aws_dbx()
    # scim_runner.get_workspaces_list()


    
    # get all inactive users in prod
    # code for finding inactive users
    # users = scim_runner.get_all_users_dbx()
    # users[users['active']==True] 
     

    # code to deduplicate spns 
    # url = 'https://accounts.cloud.databricks.com/api/2.0/accounts/290bb66c-c4bc-4a66-b7ba-14402c774119/scim/v2/ServicePrincipals'
    # req = requests.get(url, headers={'Authorization': 'Bearer ' + scim_runner.token_dbx })
    # df = pd.DataFrame(req.json()['Resources'])
    # df['order'] = df.groupby(['displayName']).cumcount()+1
    # df = df[df['order']!=1]
    # for idx, row in df.iterrows():
      
    #   url = f"https://accounts.cloud.databricks.com/api/2.0/accounts/290bb66c-c4bc-4a66-b7ba-14402c774119/scim/v2/ServicePrincipals/{row['id']}"
    #   requests.delete(url, headers={'Authorization': 'Bearer ' + scim_runner.token_dbx })
    
    # activate_list = ["-task-gderivsservice@bp365.bp.com",
    # "-tsk-reu-adw-e2extst@bp365.bp.com",
    # "dw_test_fof@bp365.bp.com",
    # "dw_test_tardis@bp365.bp.com",
    # "dw_test_user@bp365.bp.com",
    # "erv-gpti-quant-dev@bp365.bp.com",
    # "sfarcdbxdev3read@bp365.bp.com",
    # "sfdatasetdbxdev3ro@bp365.bp.com",
    # "sfdatasetdbxprodro@bp365.bp.com",
    # "sfdatasetdbxprodrw@bp365.bp.com",
    # "sfhelixdbxdev3ro@bp365.bp.com",
    # "sfhelixdbxprodro@bp365.bp.com",
    # "sfhelixdbxprodrw@bp365.bp.com",
    # "sfhpdidbxdev3ro@bp365.bp.com",
    # "sfhpdidbxdev3rw@bp365.bp.com",
    # "sfhpdidbxprodro@bp365.bp.com",
    # "sfhpdidbxprodrw@bp365.bp.com",
    # "ask-scr-auto-prod@bp365.bp.com"]
    
    # for item in activate_list:

    #   try:
    #     user_details = scim_runner.get_user_details_with_userName_dbx(item)

    #     print(user_details['id'].iloc[0])
    #     print(user_details['active'].iloc[0])

    #     url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/290bb66c-c4bc-4a66-b7ba-14402c774119/scim/v2/Users/{user_details['id'].iloc[0]}"

    #     headers = {'Authorization': 'Bearer ' + scim_runner.token_dbx }
    #     payload = {
    #             "schemas": [
    #             "urn:ietf:params:scim:api:messages:2.0:PatchOp"
    #             ],
    #             "Operations": [
    #             {
    #             "op": "replace",
    #             "value": {
    #             "active": True
    #             }
    #             }
    #             ]
    #             }
        
    #     req = requests.patch(url=url, headers=headers, json = payload)
    #   except:
    #     print('not valid')

    if base_config['deactivate_deleted_users']:
      begin = time.time()
      scim_runner.deactivate_deleted_users()
      end = time.time()
      print("Time taken to deactivate deleted users is", end-begin) 

    begin = time.time() 
    scim_runner.sync_users()
    end = time.time() 
    print("Time taken to execute the sync users is", end-begin) 

    # groups_df = scim_runner.get_all_groups_aad_member_count()

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

