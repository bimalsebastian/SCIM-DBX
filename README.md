
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

# Azure AD to Databricks SCIM Integrator

This Python utility facilitates the integration between Azure Active Directory (AAD) and Databricks Account Console using SCIM (System for Cross-domain Identity Management) APIs. It provides robust synchronization of users, groups, and service principals between AAD and Databricks.

## Key Features

### Authentication and Token Management
- Supports authentication with both Azure AD and Databricks
- Handles token refresh and expiration for both Azure and Databricks
- Supports both Azure-hosted and AWS-hosted Databricks workspaces

### Data Retrieval
- Fetches user, group, and service principal data from Azure AD
- Retrieves corresponding data from Databricks
- Supports retrieval of nested groups and their members
- Implements efficient batch processing and pagination for large datasets

### Synchronization
- Syncs users, groups, and their mappings between Azure AD and Databricks
- Handles creation, activation, deactivation, and deletion of entities in Databricks
- Supports syncing of nested group structures

### SCIM API Integration
- Utilizes SCIM APIs for identity management operations
- Supports both standard and scalable SCIM APIs, allowing management of up to 50,000 users

### Performance Optimization
- Implements multi-threading for concurrent API calls
- Uses caching mechanisms to reduce redundant API calls
- Employs batching techniques for efficient processing of large datasets

### Error Handling and Reliability
- Comprehensive error logging for debugging and monitoring
- Implements retry mechanisms for failed API calls
- Rate limiting to prevent API throttling

### Flexibility and Configuration
- Configurable synchronization options
- Supports dry run mode for simulating changes without applying them
- Customizable logging levels and output

### Additional Utilities
- Functions for analyzing group memberships and user counts
- Capability to export synchronization data for auditing purposes
- Tools for managing orphaned users and deleted accounts

## Key Components

1. **User Management**: Create, update, activate, and deactivate users in Databricks based on Azure AD data.
2. **Group Management**: Sync group structures and memberships, including nested groups.
3. **Service Principal Handling**: Manage service principals in Databricks, syncing with Azure AD applications.
4. **Scalability**: Designed to handle large enterprises with thousands of users and complex group structures.
5. **Cross-Cloud Support**: Functions with both Azure-hosted and AWS-hosted Databricks workspaces.
6. **Detailed Logging**: Comprehensive logging for auditing and troubleshooting.

## Usage

This utility is designed to be run as a scheduled job to maintain synchronization between Azure AD and Databricks. It can be configured to perform full or incremental syncs based on the organization's needs.

## Requirements

- Python 3.x
- Required Python packages: requests, pandas, numpy, msal, ratelimit

## Configuration

The tool requires configuration files for Azure AD and Databricks credentials, as well as settings for synchronization behavior. Ensure all necessary configurations are set before running the utility.

## Note

This tool performs critical identity management operations. It is recommended to thoroughly test in a non-production environment before deploying to production. Always ensure you have proper backups and rollback procedures in place.



### Other considerations,
1. Databricks Account SCIM APIs are throttled as referenced here : https://learn.microsoft.com/en-us/azure/databricks/resources/limits
2. The code user multi threading to speed up the sync, but this is throttled.
3. Retry logic is built into the code, but there are chances for missing syncs. These can be evaluated from the log files. Please adjust the Log Level in code and the Log path while initialising the code.
4. All the groups to sync are to be managed in the json file separately. This can independently generated and maintained outside the core script.
   

## Key call outs,
1. This code is not fully tested for all customer scenarios.
2. The customer owns the liability to test and evaluate the code before any use.
3. No support of this code is provided.

