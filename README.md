
# This is a python based utility to integrate Users, SPNs and Groups from Azure AAD to Databricks Accounts Console.
## The code works on the following logic

### Pre Requisites:
* List of all the AD groups that needs to be synced into Databricks.
* An SPN / AD application that can be used to read information from Azure AD with respective privileges (Read Users, SPNs and Groups)
* The SPN also needs to be configured to interact with the Databricks Account console with an admin privilege.

### Core Logic:
* Takes the input from the list groups_to_sync: Feed in all the AD groups you need to sync into databricks account console
* The utility then reads all the users, SPNs and groups listed as children within these groups from Azure AD  / Entra ID using the Service Principal credential. Using the dictionary : "config" to configure this
* Once the data is read, it synchronises all users into Databricks starting with creating any new users, groups, SPNs and updating their group membership (Nested group included). This synchronisation also includes SPNs
* It then moves on to create all the groups including the nested groups.
* Once the base set is created, it moves on to synchronising the relations between Users, SPNs and other child groups into respective groups.

scim_runner is the main entry into the implementation logic. It shows how the base class scim_integrator is created and the underlying functions for synchronisation is called

### Key Configurations in yaml

```
config:
  client_id: <ClientID from Azure SPN>
  client_secret: <Client Secret from Azure SPN>,
  authority: https://login.microsoftonline.com/<AZURE TENANT ID>,
  scope: ['https://graph.microsoft.com/.default']


dbx_config: 
  dbx_host: <Databricks Workspace URL>
  dbx_account_host: 'https://accounts.azuredatabricks.net'
  account_id: <Databricks AccountID>
  azure_tenant_id: <Azure Tenant ID>
  client_id: <Client ID for azure SPN>
  client_secret: <Client Secret for azure SPN>

LOG_FILE_LOCATION: '/SourceCode/tmp/'
LOG_FILE_NAME : log

groups_to_sync_path : 'groups_to_sync.json'

is_dryrun: True

deactivate_deleted_users : True

deactivate_orphan_users : True
```
### Major Features

1. The code does not manage account_admins. These Ids have to be managed separately.
2. A dry run feature exists (highly recommended). Use the flag is_dryrun to enable this. This will give a count of all major changes the utility would do. This include (no of users, groups, spns to be created, activtated and the group membership that needs to be updated
3. Deacivation of users are explicility commented out in this code to give better control during UC migration
4. Deactivation of users deleted in Azure are handled through the flag : deactivate_deleted_users
5. Deactivation of orphan users (i.e. users that dont belong to any groups) are handled through the flag : deactivate_orphan_users


### Other considerations,
1. Databricks Account SCIM APIs are throttled as referenced here : https://learn.microsoft.com/en-us/azure/databricks/resources/limits
2. The code user multi threading to speed up the sync, but this is throttled.
3. Retry logic is built into the code, but there are chances for missing syncs. These can be evaluated from the log files. Please adjust the Log Level in code and the Log path while initialising the code.
4. All the groups to sync are to be managed in the json file separately. This can independently generated and maintained outside the core script.
   

## Key call outs,
1. This code is not fully tested for all customer scenarios.
2. The customer owns the liability to test and evaluate the code before any use.
3. No support of this code is provided.

