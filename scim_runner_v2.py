#!/usr/bin/env python3

import asyncio

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

config_path = "/Users/bimal.sebastian/SourceCode/SCIM Utility/config.yml"
# config_path =  '/Users/bimal.sebastian/SourceCode/BP UC Workspace ACL-Performance/configs/EAScriptPython_config.yml'


async def call_v2():

  from scim_integrator_async import SCIMIntegrator as scim_integrator
  from scim_integrator_async import AADConfig, DBXConfig
  config = ''
    
  with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
      _aad_config = AADConfig(client_id=base_config['config']['client_id'],
                            client_secret=base_config['config']['client_secret'],
                            authority=base_config['config']['authority'],
                            scope=base_config['config']['scope'])

      _dbx_config = DBXConfig(
                              dbx_account_host=base_config['dbx_config']['dbx_account_host'],
                              account_id=base_config['dbx_config']['account_id'],
                              azure_tenant_id=base_config['dbx_config']['azure_tenant_id'],
                              client_id=base_config['dbx_config']['client_id'],
                              client_secret=base_config['dbx_config']['client_secret'],
                              account_get_resource_limit=base_config['dbx_config']['account_get_resource_limit'],
                              account_post_resource_limit=base_config['dbx_config']['account_post_resource_limit'],
                              account_patch_resource_limit=base_config['dbx_config']['account_patch_resource_limit'],
                              aws_user_id=base_config['dbx_config']['aws_user_id'],
                              aws_password=base_config['dbx_config']['aws_password'],
                              workspace_client_secret = '')

  if base_config['extract_all_nested_groups']:
    with open(base_config['groups_to_sync_path']) as file:
      groups_to_sync = json.load(file)
    nested_runner = scim_integrator(_aad_config,
                                  _dbx_config,
                                  groups_to_sync ,base_config['LOG_FILE_NAME'] ,
                                  base_config['LOG_FILE_LOCATION'],
                                  is_dryrun =  base_config['is_dryrun'], 
                                  Scalable_SCIM_Enabled = True, 
                                  cloud_provider ='AWS')

    await nested_runner.populate_groups_to_sync(base_config['groups_to_sync_path'])


  with open(base_config['groups_to_sync_path']) as file:
    groups_to_sync = json.load(file)
      

  scim_runner = scim_integrator(_aad_config,
                                _dbx_config,
                                groups_to_sync ,base_config['LOG_FILE_NAME'] ,
                                base_config['LOG_FILE_LOCATION'],
                                is_dryrun =  base_config['is_dryrun'], 
                                Scalable_SCIM_Enabled = True, 
                                cloud_provider ='AWS')



  if base_config['deactivate_deleted_users']:
    begin = time.time()
    await scim_runner.deactivate_deleted_users()
    end = time.time()
    print("Time taken to deactivate deleted users is", end-begin) 

  begin = time.time() 
  await scim_runner.sync_users()
  end = time.time() 
  print("Time taken to execute the sync users is", end-begin) 

  # groups_df = scim_runner.get_all_groups_aad_member_count()

  begin = time.time() 
  await scim_runner.sync_groups()
  end = time.time() 
  print("Time taken to execute the sync groups is", end-begin) 
  

  begin = time.time() 
  await scim_runner.sync_mappings()
  end = time.time() 
  print("Time taken to execute the sync mappings is", end-begin) 

  if base_config['deactivate_orphan_users']:
    begin = time.time() 
    await scim_runner.deactivate_orphan_users()
    end = time.time() 
    print("Time taken to deactivate orphan users is", end-begin) 

def call_v1():

  from scim_integrator import scim_integrator
  config = ''
    
  with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)

  if base_config['extract_all_nested_groups']:
    with open(base_config['groups_to_sync_path']) as file:
      groups_to_sync = json.load(file)
    nested_runner = scim_integrator(base_config['config'],
                                  base_config['dbx_config'],
                                  groups_to_sync ,base_config['LOG_FILE_NAME'] ,
                                  base_config['LOG_FILE_LOCATION'],
                                  is_dryrun =  base_config['is_dryrun'], 
                                  Scalable_SCIM_Enabled = True, 
                                  cloud_provider ='AWS')

    
    nested_runner.populate_groups_to_sync(base_config['groups_to_sync_path'])


  with open(base_config['groups_to_sync_path']) as file:
    groups_to_sync = json.load(file)
      

  scim_runner = scim_integrator(base_config['config'],
                                base_config['dbx_config'],
                                groups_to_sync ,base_config['LOG_FILE_NAME'] ,
                                base_config['LOG_FILE_LOCATION'],
                                is_dryrun =  base_config['is_dryrun'], 
                                Scalable_SCIM_Enabled = True, 
                                cloud_provider ='AWS')



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

if __name__ == '__main__':
    begin = time.time()
    asyncio.run(call_v2())
    end = time.time() 
    print('Begining Asycn based sync : Time : ',  end-begin)

    # begin = time.time()
    # print('Begining regular sync')
    # call_v1()
    # end = time.time() 
    # print('Begining Asycn based sync : Time : ',  end-begin)

    
