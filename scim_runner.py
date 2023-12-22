from scim_integrator import scim_integrator
import time 

LOG_FILE_LOCATION = '/Users/bimal.sebastian/SourceCode/tmp/'
LOG_FILE_NAME = 'log'

# This is the configuration to access azure Graph API.
config = {
  'client_id' : '<ClientID from Azure SPN>',
  'client_secret' : '<Client Secret from Azure SPN>',
  'authority' : 'https://login.microsoftonline.com/<AZURE TENANT ID>',
  'scope' : ['https://graph.microsoft.com/.default']
}

# This is the configuration to access Databricks Account SCIM.
dbx_config = {
    'dbx_host' : "<Databricks Workspace URL>",
    'dbx_account_host' : 'https://accounts.azuredatabricks.net',
    'account_id' : '<Databricks AccountID>',
    'azure_tenant_id' : '<Azure Tenant ID>',
    'client_id' : 'Client ID for azure SPN',
    'client_secret' : 'Client Secret for azure SPN'
}

groups_to_sync = ['Nested Group 1','Nested Group 2','Nested Group 3','Nested Level 2','Nested Level 2_2','DummyGroup0','DummyGroup100',	'dummygroup1',	'dummygroup2',	'dummygroup3',	'dummygroup4',	'dummygroup5',	'dummygroup6',	'dummygroup7',	'dummygroup8',	'dummygroup9',	'dummygroup10',	'dummygroup11',	'dummygroup12',	'dummygroup13',	'dummygroup14',	'dummygroup15',	'dummygroup16',	'dummygroup17',	'dummygroup18',	'dummygroup19',	'dummygroup20',	'dummygroup21',	'dummygroup22',	'dummygroup23',	'dummygroup24',	'dummygroup25',	'dummygroup26',	'dummygroup27',	'dummygroup28',	'dummygroup29',	'dummygroup30',	'dummygroup31',	'dummygroup32',	'dummygroup33',	'dummygroup34',	'dummygroup35',	'dummygroup36',	'dummygroup37',	'dummygroup38',	'dummygroup39',	'dummygroup40',	'dummygroup41',	'dummygroup42',	'dummygroup43',	'dummygroup44',	'dummygroup45',	'dummygroup46',	'dummygroup47',	'dummygroup48',	'dummygroup49',	'dummygroup50',	'dummygroup51',	'dummygroup52',	'dummygroup53',	'dummygroup54',	'dummygroup55',	'dummygroup56',	'dummygroup57',	'dummygroup58',	'dummygroup59',	'dummygroup60',	'dummygroup61',	'dummygroup62',	'dummygroup63',	'dummygroup64',	'dummygroup65',	'dummygroup66',	'dummygroup67',	'dummygroup68',	'dummygroup69',	'dummygroup70',	'dummygroup71',	'dummygroup72',	'dummygroup73',	'dummygroup74',	'dummygroup75',	'dummygroup76',	'dummygroup77',	'dummygroup78',	'dummygroup79',	'dummygroup80',	'dummygroup81',	'dummygroup82',	'dummygroup83',	'dummygroup84',	'dummygroup85',	'dummygroup86',	'dummygroup87',	'dummygroup88',	'dummygroup89',	'dummygroup90',	'dummygroup91',	'dummygroup92',	'dummygroup93',	'dummygroup94',	'dummygroup95',	'dummygroup96',	'dummygroup97',	'dummygroup98',	'dummygroup99']


if __name__ == '__main__':
    scim_runner = scim_integrator(config,dbx_config,groups_to_sync,LOG_FILE_NAME,LOG_FILE_LOCATION)
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


 
    