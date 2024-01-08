import pytest
from scim_integrator import scim_integrator as scim_integrator
import yaml
import json

config_path = "../Alternate Configs/config bimal.yml"

def test_init():
     
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
    with open(base_config['groups_to_sync_path']) as file:
       groups_to_sync = json.load(file)
    
    scim_runner = scim_integrator(base_config['config'],base_config['dbx_config'],groups_to_sync ,base_config['LOG_FILE_NAME'] ,base_config['LOG_FILE_LOCATION'],is_dryrun =  base_config['is_dryrun'])
    assert scim_runner is not None
 
def test_auth_dbx_account():
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
    with open(base_config['groups_to_sync_path']) as file:
       groups_to_sync = json.load(file)
    scim_runner = scim_integrator(base_config['config'],base_config['dbx_config'],groups_to_sync ,base_config['LOG_FILE_NAME'] ,base_config['LOG_FILE_LOCATION'],is_dryrun =  base_config['is_dryrun'])
    scim_runner.auth_aad(True)
    assert scim_runner.token_dbx != ''

def test_auth_aad():
    config = ''
    with open(config_path, 'r') as file:
      base_config = yaml.safe_load(file)
    with open(base_config['groups_to_sync_path']) as file:
       groups_to_sync = json.load(file)
    scim_runner = scim_integrator(base_config['config'],base_config['dbx_config'],groups_to_sync ,base_config['LOG_FILE_NAME'] ,base_config['LOG_FILE_LOCATION'],is_dryrun =  base_config['is_dryrun'])
    scim_runner.auth_aad(False)
    assert scim_runner.token != ''