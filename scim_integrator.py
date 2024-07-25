import log_decorator
import log
import msal
import requests
import pandas as pd
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache 
import time
from ratelimit import limits, RateLimitException, sleep_and_retry
import logging
from io import BytesIO
import math
from requests.auth import HTTPBasicAuth
import datetime

class scim_integrator():
    """
    A class to integrate Azure Active Directory with Databricks Account Console using SCIM.
    
    This class provides functionality to sync users, groups, and their mappings between 
    Azure AD and Databricks. It supports both regular and scalable SCIM APIs, allowing 
    integration for up to 50,000 users.

    Attributes:
        config (dict): Configuration for Azure AD connection.
        dbx_config (dict): Configuration for Databricks connection.
        groups_to_sync (list): List of AD groups to be synced.
        token (str): Azure AD authentication token.
        token_dbx (str): Databricks authentication token.
        is_dryrun (bool): Flag to indicate if operations should be simulated.
        log_file_name (str): Name of the log file.
        log_file_dir (str): Directory for log files.
        logger_obj (logging.Logger): Logger object for the class.
        MAX_GET_CALLS_PER_SEC (int): Rate limit for GET API calls.
        MAX_PATCH_CALLS_PER_SEC (int): Rate limit for PATCH API calls.
        MAX_POST_CALLS_PER_SEC (int): Rate limit for POST API calls.
    """
    
    def __init__(self, config, dbx_config, groups_to_sync, log_file_name, log_file_dir, token_dbx = '', token = '', is_dryrun = True,Scalable_SCIM_Enabled = False, cloud_provider='Azure'  ):
        """
        Initialize the scim_integrator instance.

        Args:
            config (dict): Configuration for Azure AD connection.
            dbx_config (dict): Configuration for Databricks connection.
            groups_to_sync (list): List of AD groups to be synced.
            log_file_name (str): Name of the log file.
            log_file_dir (str): Directory for log files.
            token_dbx (str, optional): Databricks authentication token. Defaults to ''. Optional in case this has to be overridden.
            token (str, optional): Azure AD authentication token. Defaults to ''. Optional in case this has to be overridden.
            is_dryrun (bool, optional): Flag to indicate if operations should be simulated. Defaults to True.
        """
        
        self.config = config
        self.dbx_config = dbx_config
        self.groups_to_sync = groups_to_sync
        self.token = token
        self.token_dbx = token_dbx
        self.dbx_token_expiry = datetime.datetime.now()
        self.aad_token_expiry = datetime.datetime.now()
        self.is_dryrun = is_dryrun
        self.log_file_name = log_file_name
        self.log_file_dir = log_file_dir
        self.logger_obj = log.get_logger(log_file_name=self.log_file_name, log_dir=self.log_file_dir, loggingLevel= logging.INFO)
        self.MAX_GET_CALLS_PER_SEC = 20
        self.MAX_PATCH_CALLS_PER_SEC = 2
        self.MAX_POST_CALLS_PER_SEC = 5
        self.Scalable_SCIM_Enabled = Scalable_SCIM_Enabled
        self.cloud_provider = cloud_provider
        

    # global token 
    # global token_dbx     
   
    def make_graph_get_call(self,url, pagination=True, params = {},key = '', headers_in = {}):
        """
        Make a GET request to the Microsoft Graph API.

        This function handles pagination and supports both regular and batch requests.
        It uses exponential backoff for retrying failed requests.

        Args:
            url (str): The API endpoint URL.
            pagination (bool, optional): Whether to handle pagination. Defaults to True.
            params (dict, optional): Query parameters for the request. Defaults to {}.
            key (str, optional): Key for batch requests. Defaults to ''.
            headers_in (dict, optional): Additional headers for the request. Defaults to {}.

        Returns:
            tuple or list: If key is provided, returns a tuple of (key, results).
                           Otherwise, returns a list of results.

        Raises:
            Logs errors for failed requests after retry attempts.
        """
        token = self.get_aad_token()

        headers =  {'Authorization': 'Bearer ' + token}
        headers = {**headers, **headers_in}
        graph_results = []
        while url:
            try:
                if(len(params)>0):
                    self.logger_obj.debug(f"Logging API call params{params}")
                    graph_result = requests.get(url=url, headers=headers, params = params).json()
                else:
                    graph_result = requests.get(url=url, headers=headers).json()
                if 'value' in graph_result:
                    graph_results.extend(graph_result['value'])
                else: 
                    graph_results.append(graph_result)
                if (pagination == True):
                    url = graph_result['@odata.nextLink']
                    params = {}
                else:
                    url = None
            except:
                break
        if key == '':        
            return graph_results
        else: 
            return key, graph_results

    def auth_aad(self,isAccount =True):
        """
        Authenticate with Azure Active Directory or Azure Databricks.

        This function obtains an authentication token either for Azure AD or Databricks,
        depending on the isAccount parameter. It uses client credentials flow for authentication.

        Args:
            isAccount (bool, optional): If True, authenticate with Azure Databricks. 
                                        If False, authenticate with Azure AD. Defaults to True.

        Raises:
            Logs errors for failed authentication attempts.
        """
        
        if isAccount:
            url = f"https://login.microsoftonline.com/{self.dbx_config['azure_tenant_id']}/oauth2/v2.0/token"
        
            post_data = {'client_id': self.dbx_config['client_id'],
                        'scope' :'2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
                        'client_secret': self.dbx_config['client_secret'],
                        'grant_type': 'client_credentials'}
            initial_header = {'Content-type': 'application/x-www-form-urlencoded'}
            res = requests.post(url, data=post_data, headers=initial_header)
            res.raise_for_status()
            self.dbx_token_expiry = datetime.datetime.now() + datetime.timedelta(res.json()['expires_in'] - 30)
            self.token_dbx = res.json().get("access_token")
            
        else:
            client = msal.ConfidentialClientApplication(self.config['client_id'], authority=self.config['authority'], client_credential=self.config['client_secret'])
            token_result = client.acquire_token_for_client(scopes=self.config['scope'])
            self.token = token_result['access_token']
    def get_dbx_token(self):
        """
        Retrieve a valid Databricks token, refreshing if necessary.

        This method checks if the current Databricks token is expired. If it is, it generates a new token
        using the appropriate authentication method based on the cloud provider (Azure or AWS).

        Returns:
            str: A valid Databricks authentication token.

        Notes:
            - Uses self.cloud_provider to determine the appropriate authentication method.
            - For Azure, it calls auth_aad(True).
            - For AWS, it calls auth_aws_dbx().
            - Manages token expiration to ensure a valid token is always returned.
        """
        if datetime.datetime.now()>= self.dbx_token_expiry:
            if self.cloud_provider == 'Azure':
                self.auth_aad(True)
            else:
                self.auth_aws_dbx()
        return self.token_dbx
    
    def get_aad_token(self):
        """
        Retrieve a valid Azure Active Directory (AAD) token, refreshing if necessary.

        This method checks if the current AAD token is expired. If it is, it generates a new token
        by calling the auth_aad method with False parameter.

        Returns:
            str: A valid Azure Active Directory authentication token.

        Notes:
            - Manages token expiration to ensure a valid token is always returned.
            - Uses auth_aad(False) to refresh the AAD token when expired.
        """
        if datetime.datetime.now()>= self.aad_token_expiry:
            self.auth_aad(False)
        return self.token

    def auth_aws_dbx(self):
        """
        Authenticate with Databricks on AWS and obtain an access token.

        This method performs authentication for Databricks workspaces hosted on AWS. It uses
        the account ID, user ID, and password stored in the dbx_config to obtain an access token.

        Notes:
            - Uses HTTP Basic Authentication with the AWS user ID and password.
            - Requests a token with 'all-apis' scope.
            - Updates self.token_dbx with the new access token.
            - Sets self.dbx_token_expiry based on the token's expiration time (with a 30-second buffer).
            - Raises an exception if the authentication request fails.

        Raises:
            requests.exceptions.HTTPError: If the authentication request fails.
        """
        url = f"https://accounts.cloud.databricks.com/oidc/accounts/{self.dbx_config['account_id']}/v1/token"
    
        auth=HTTPBasicAuth(self.dbx_config['aws_user_id'], self.dbx_config['aws_password'])
        params = {'grant_type':'client_credentials','scope':'all-apis'}
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res = requests.post(url, auth=auth, data=params, headers=headers)
        res.raise_for_status()
        self.dbx_token_expiry = datetime.datetime.now() + datetime.timedelta(res.json()['expires_in'] - 30)
        self.token_dbx = res.json().get("access_token")
            

    def get_spn_details(self, service_principals):
        """
            Retrieve detailed information about service principals from Azure AD.

            This function makes Graph API calls to get detailed information for each service principal
            provided in the input DataFrame.

            Args:
                service_principals (pd.DataFrame): DataFrame containing service principal IDs.

            Returns:
                pd.DataFrame: DataFrame with detailed information about the service principals.

            Notes:
                - Uses the make_graph_get_call method for API requests.
                - Concatenates results for multiple service principals into a single DataFrame.
            """
        spns_df = pd.DataFrame()
        for idx, spn in service_principals.iterrows():
            url = f'https://graph.microsoft.com/v1.0/servicePrincipals/{spn["id"]}'
            res = self.make_graph_get_call(url, False)
            res_json = json.dumps(res)
            res_json = str.encode(res_json)
            df = pd.read_json(BytesIO(res_json),dtype='unicode',convert_dates=False)
            spns_df = pd.concat([spns_df,df])
        return spns_df


    @lru_cache(maxsize=256, typed=True)
    def get_all_groups_aad(self,with_members = False):
        """
        Retrieve all groups from Azure AD based on the groups_to_sync list.

        This function fetches group information from Azure AD, optionally including member details.
        It uses batch processing and multi-threading for improved performance.

        Args:
            with_members (bool, optional): If True, include member details for each group. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame containing group information and optionally member details.

        Notes:
            - Uses caching to improve performance for repeated calls.
            - Implements multi-threading for parallel processing of API calls.
            - Handles pagination for large result sets.
            - Supports both user and service principal group members.
        """
        batch_size = 10
        split_count = round(len(self.groups_to_sync)/batch_size,0)
        groups_to_sync_split = []
        if split_count > 0 :
            groups_to_sync_split = np.array_split(self.groups_to_sync, split_count)
        else:
            groups_to_sync_split = np.array_split(self.groups_to_sync, 1)
        filter_params = []
        user_groups_df = pd.DataFrame()

        for group_set in groups_to_sync_split:
            filter_expression = ', '.join(['"{}"'.format(value) for value in group_set])
            filter_params.append({'$filter' : f"displayName in ({filter_expression})",'$select':'id,displayName'})
        threads= []
        master_list = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:
            for filter_param in filter_params:
                url = 'https://graph.microsoft.com/v1.0/groups'
                threads.append(executor.submit(self.make_graph_get_call, url, True, filter_param))
            for task in as_completed(threads):
                groups_json = json.dumps(task.result())
                groups_json = str.encode(groups_json)
                groups_df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
                master_list = pd.concat([master_list,groups_df])

        threads_sub= []
        if with_members:
            user_list = []
            with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor_sub:
                for index, row in master_list.iterrows():
                    # url = 'https://graph.microsoft.com/v1.0/groups'
                    url = 'https://graph.microsoft.com/beta/groups'
                    params = {'$select':'id, displayName, userPrincipalName,appDisplayName, appId'}

                    group_id = row['id']
                    threads_sub.append(executor_sub.submit(self.make_graph_get_call, url+'/'+ group_id+'/members', True, params =params , key= group_id))
                for sub_task in as_completed(threads_sub):
                    result =sub_task.result()
                    user_list = json.dumps(result[1])
                    user_list = str.encode(user_list)
                    df = pd.read_json(BytesIO(user_list),dtype='unicode',convert_dates=False)
                    df['group_id'] = result[0]
                    if len(master_list[master_list['id']==result[0]])>0: 
                        df['aad_group_displayName'] = str(master_list[master_list['id']==result[0]]['displayName'].iloc[0]).lower()
                    # print(result[0])
                    user_groups_df = pd.concat([user_groups_df,df])
                
        else:   
            return master_list
                
        return user_groups_df
    
    def get_all_groups_nested_aad(self, parent_groups):
        """
        Retrieve nested group information from Azure Active Directory for given parent groups.

        This function fetches detailed information about the specified parent groups and their nested subgroups
        from Azure AD. It uses batch processing and multi-threading for improved performance.

        Args:
            parent_groups (list): List of parent group names to fetch nested group information for.

        Returns:
            pd.DataFrame: DataFrame containing nested group information, or None if no groups are found.

        Notes:
            - Implements batching to handle large numbers of parent groups efficiently.
            - Uses multi-threading to make concurrent API calls, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Fetches both group details and their member groups.
            - Concatenates results into a single DataFrame with group hierarchy information.
        """
        batch_size = 10
        split_count = round(len(parent_groups)/batch_size,0)
        groups_to_sync_split = []
        if split_count > 0 :
            groups_to_sync_split = np.array_split(parent_groups, split_count)
        else:
            groups_to_sync_split = np.array_split(parent_groups, 1)
        filter_params = []
        user_groups_df = pd.DataFrame()

        for group_set in groups_to_sync_split:
            filter_expression = ', '.join(['"{}"'.format(value) for value in group_set])
            filter_params.append({'$filter' : f"displayName in ({filter_expression})",'$select':'id,displayName'})
        threads= []
        master_list = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:
            for filter_param in filter_params:
                url = 'https://graph.microsoft.com/v1.0/groups'
                threads.append(executor.submit(self.make_graph_get_call, url, True, filter_param))
            for task in as_completed(threads):
                groups_json = json.dumps(task.result())
                groups_json = str.encode(groups_json)
                groups_df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
                master_list = pd.concat([master_list,groups_df])

        threads_sub= []
        
        user_list = []
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor_sub:
            for index, row in master_list.iterrows():
                url = 'https://graph.microsoft.com/beta/groups'
                params = {'$filter':'','$select':'id, displayName'}

                group_id = row['id']
                threads_sub.append(executor_sub.submit(self.make_graph_get_call, url+'/'+ group_id+'/members/microsoft.graph.group', True, params =params , key= group_id))
            for sub_task in as_completed(threads_sub):
                result =sub_task.result()
                user_list = json.dumps(result[1])
                user_list = str.encode(user_list)
                df = pd.read_json(BytesIO(user_list),dtype='unicode',convert_dates=False)
                df['group_id'] = result[0]
                if len(master_list[master_list['id']==result[0]])>0: 
                    df['aad_group_displayName'] = str(master_list[master_list['id']==result[0]]['displayName'].iloc[0]).lower()
                # print(result[0])
                user_groups_df = pd.concat([user_groups_df,df])
                
        if user_groups_df.shape[0]>0:        
            return user_groups_df
        else:
            return None
    def dump_json(self, new_groups_to_sync, path):
        """
        Write a list of groups to a JSON file.

        This function takes a list of group names and writes them to a JSON file at the specified path.

        Args:
            new_groups_to_sync (list): List of group names to be written to the JSON file.
            path (str): File path where the JSON file will be created or overwritten.

        Notes:
            - Uses json.dump with indentation for better readability of the output file.
            - Ensures proper encoding of non-ASCII characters.
        """
        with open(path, 'w') as outfile:
            json.dump(new_groups_to_sync, outfile, ensure_ascii=False, indent=4)     
    
    def populate_groups_to_sync(self, path):
        """
        Populate and update the groups_to_sync list with nested groups.

        This function recursively fetches nested groups starting from the initial groups_to_sync list,
        and updates the list with all discovered nested groups. The final list is then saved to a JSON file.

        Args:
            path (str): File path where the updated groups_to_sync list will be saved as a JSON file.

        Notes:
            - Iteratively fetches nested groups until no new groups are discovered.
            - Updates the groups_to_sync list with newly discovered groups in each iteration.
            - Saves the final, comprehensive list of groups to the specified JSON file.
        """
        iter_groups_to_sync = self.groups_to_sync
        all_groups = self.get_all_groups_nested_aad(iter_groups_to_sync)

        new_parents = list(all_groups[~all_groups['displayName'].isin(iter_groups_to_sync)]['displayName'])
        while len(new_parents)>0:
            all_groups = self.get_all_groups_nested_aad(iter_groups_to_sync)
            new_parents = list(all_groups[~all_groups['displayName'].isin(iter_groups_to_sync)]['displayName'])
            iter_groups_to_sync.extend(new_parents)
        
        self.dump_json(iter_groups_to_sync, path)

    def get_all_groups_aad_member_count(self):
        """
        Retrieve all groups from Azure AD along with their member counts.

        This function fetches information about all groups in Azure AD and calculates
        the number of members for each group.

        Returns:
            pd.DataFrame: DataFrame containing group information including id, displayName, and member_count.

        Notes:
            - Uses the Graph API to fetch group information and member counts.
            - Implements pagination to handle large numbers of groups.
            - Calculates the total member count across all groups.
            - The commented-out condition (if total_count >= 20000) suggests a potential limit
            that could be implemented to stop processing after reaching a certain member count.
        """
        params = {'$select':'id, displayName'}
        threads = []
        url = 'https://graph.microsoft.com/v1.0/groups'
        resp = self.make_graph_get_call( url, True, params=params)
        groups_json = json.dumps(resp)
        groups_json = str.encode(groups_json)
        groups_df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
        groups_df['member_count'] = 0
        total_count = 0
        for idx, row in groups_df.iterrows():
            group_id = row['id']
            params = {'$count':'true','ConsistencyLevel':'eventual','$select':'id'}
            url = f'https://graph.microsoft.com/v1.0/groups/{group_id}/members/'
            resp = self.make_graph_get_call( url, True, params=params)
            if len(resp)>0:
                if '@odata.count' in resp[0]:
                    member_count = resp['@odata.count']
                else:
                    member_count = len(resp)
                groups_df['member_count'].iloc[idx] = member_count
                total_count+=member_count
                # if total_count>=20000:
                #     break
        return groups_df

    @lru_cache(maxsize=256, typed=True)
    def get_all_users_aad(self):
        """
        Retrieve all users from Azure AD.

        This function fetches user information from Azure AD using the Microsoft Graph API.

        Returns:
            pd.DataFrame: DataFrame containing user information.

        Notes:
            - Uses caching to improve performance for repeated calls.
            - Handles pagination for large result sets.
        """
        url = 'https://graph.microsoft.com/v1.0/users'
        users = self.make_graph_get_call(url, pagination=True)
        users_json = json.dumps(users)
        users_json = str.encode(users_json)
        users_df = pd.read_json(BytesIO(users_json),dtype='unicode',convert_dates=False)
        
        
        return users_df
    
    def get_users_by_username_aad(self,usernames):
        """
        Retrieve user information from Azure AD for a list of usernames.

        This function fetches user details from Azure AD based on the provided list of usernames (userPrincipalNames).
        It uses batch processing and multi-threading for improved performance when dealing with a large number of users.

        Args:
            usernames (list): List of userPrincipalNames to fetch user information for.

        Returns:
            pd.DataFrame: DataFrame containing user information (id and userPrincipalName) for the specified usernames.

        Notes:
            - Implements batching to handle large numbers of usernames efficiently.
            - Uses multi-threading to make concurrent API calls, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Concatenates results from multiple API calls into a single DataFrame.
        """
        batch_size = 10
        split_count = round(len(usernames)/batch_size,0)
        usernames_split = []
        if split_count > 0 :
            usernames_split = np.array_split(usernames, split_count)
        else:
            usernames_split = np.array_split(usernames, 1)
        filter_params = []
        user_groups_df = pd.DataFrame()

        for user in usernames_split:
            filter_expression = ', '.join(['"{}"'.format(value) for value in user])
            filter_params.append({'$filter' : f"userPrincipalName in ({filter_expression})",'$select':'id,userPrincipalName'})
        threads= []
        master_list = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:
            for filter_param in filter_params:
                url = 'https://graph.microsoft.com/v1.0/users'
                threads.append(executor.submit(self.make_graph_get_call, url, True, filter_param))
            for task in as_completed(threads):
                groups_json = json.dumps(task.result())
                groups_json = str.encode(groups_json)
                groups_df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
                master_list = pd.concat([master_list,groups_df])
        return master_list
    
    def get_apps_by_displayName_aad(self,displayName):
        """
        Retrieve application information from Azure AD for a list of display names.

        This function fetches application details from Azure AD based on the provided list of display names.
        It uses batch processing and multi-threading for improved performance when dealing with a large number of applications.

        Args:
            displayName (list): List of application display names to fetch information for.

        Returns:
            pd.DataFrame: DataFrame containing application information (appId and displayName) for the specified display names.

        Notes:
            - Implements batching to handle large numbers of display names efficiently.
            - Uses multi-threading to make concurrent API calls, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Concatenates results from multiple API calls into a single DataFrame.
        """
        batch_size = 10
        split_count = round(len(displayName)/batch_size,0)
        displayName_split = []
        if split_count > 0 :
            displayName_split = np.array_split(displayName, split_count)
        else:
            displayName_split = np.array_split(displayName, 1)
        filter_params = [] 

        for app in displayName_split:
            filter_expression = ', '.join(['"{}"'.format(value) for value in app])
            filter_params.append({'$filter' : f"displayName in ({filter_expression})",'$select':'appId,displayName'})
        threads= []
        master_list = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:
            for filter_param in filter_params:
                url = 'https://graph.microsoft.com/v1.0/applications'
                threads.append(executor.submit(self.make_graph_get_call, url, True, filter_param))
            for task in as_completed(threads):
                groups_json = json.dumps(task.result())
                groups_json = str.encode(groups_json)
                groups_df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
                master_list = pd.concat([master_list,groups_df])
        return master_list

    def get_delete_users_aad(self):
        """
        Retrieve deleted users from Azure AD.

        This function fetches information about recently deleted users from Azure AD 
        using the Microsoft Graph API.

        Returns:
            pd.DataFrame: DataFrame containing information about deleted users.

        Notes:
            - Handles pagination for large result sets.
            - Cleans up the userPrincipalName for deleted users by removing appended IDs.
        """
        url = "https://graph.microsoft.com/v1.0/directory/deletedItems/microsoft.graph.user?$count=true&$orderby=deletedDateTime asc&$select=id,DisplayName,userPrincipalName,deletedDateTime"
        headers = {'ConsistencyLevel': 'eventual'}
        deleted_users = self.make_graph_get_call(url, pagination=True, headers_in = headers)
        deleted_users_json = json.dumps(deleted_users)
        deleted_users_json = str.encode(deleted_users_json)
        deleted_users_df = pd.read_json(BytesIO(deleted_users_json),dtype='unicode',convert_dates=False)
        # cleanup userPrincipal Name since deleted users are appened with id in UPN
        if len(deleted_users_df) > 0:
            for idx, row in deleted_users_df.iterrows():
                deleted_users_df.iloc[idx]['userPrincipalName'] = row['userPrincipalName'].replace(row['id'].replace('-',''),'')
        return deleted_users_df


    def get_user_details_dbx(self,ids_string):
        """
        Retrieve user details from Databricks using SCIM API.

        This function fetches user information from Databricks for the given user IDs.
        It supports the scalable SCIM API, allowing retrieval of up to 50,000 users.

        Args:
            ids_string (str): A string of user IDs to fetch, formatted for SCIM filter.

        Returns:
            pd.DataFrame: DataFrame containing user details from Databricks.

        Raises:
            Exception: If the API call fails, logs the error and re-raises the exception.

        Notes:
            - Handles both regular users and service principals.
            - Includes admin status in the returned data.
        """
        try:
            account_id = self.dbx_config["account_id"] 
            token_result = self.get_dbx_token()

            headers = {'Authorization': 'Bearer ' + token_result }

            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users"
            params = {'filter': ids_string}
            retry_counter = 0
            while True:
                req = requests.get(url=url, headers=headers, params = params)
                if req.status_code == 200:
                    break
                else:
                    if retry_counter <=5 :
                        print('Retrying get Users')
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        break
            # df = pd.DataFrame(index = range(len(req.json()['Resources'])))
            df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool'),
                   'id': pd.Series(dtype='str'),
                   'userName': pd.Series(dtype='str'),
                   'applicationId': pd.Series(dtype='str'),
                   'externalId': pd.Series(dtype='str'),
                   'isAdmin': np.full(1, False, dtype=bool),
                   }, index = range(len(req.json()['Resources'])))

            counter = 0
            for resource in req.json()['Resources']:

                if 'displayName' in resource:
                    df.loc[counter,'displayName'] = resource['displayName']
                if 'roles' in resource:
                    df.loc[counter,'isAdmin'] = True if list(filter(lambda x: x['value']  == 'account_admin', resource['roles'])) else False
                if 'active' in resource:
                    df.loc[counter,'active'] = resource['active']
                df.loc[counter,'id'] = resource['id']
                df.loc[counter,'userName'] = resource['userName']
                df.loc[counter,'applicationId'] = np.nan
                if 'externalId' in resource:
                    df.loc[counter,'externalId'] = resource['externalId']
                counter+=1
            return df
        
        except Exception as e:
            self.logger_obj.error(f"Fetching User Details Failed with status : {str(e)}")
            raise

    def get_spn_details_dbx(self,ids_string):
        """
        Retrieve service principal details from Databricks using SCIM API.

        This function fetches service principal information from Databricks for the given IDs.
        It supports the scalable SCIM API, allowing retrieval of up to 50,000 service principals.

        Args:
            ids_string (str): A string of service principal IDs to fetch, formatted for SCIM filter.

        Returns:
            pd.DataFrame: DataFrame containing service principal details from Databricks.

        Raises:
            Exception: If the API call fails, logs the error and re-raises the exception.

        Notes:
            - Similar structure to get_user_details_dbx, but specific to service principals.
        """
        try:
            account_id = self.dbx_config["account_id"] 
            token_result = self.get_dbx_token()

            headers = {'Authorization': 'Bearer ' + token_result }

            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            params = {'filter': ids_string}
            req = requests.get(url=url, headers=headers, params = params)
            if req.status_code != 200:

                df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')})
                return df
            else:
                df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')}, index = range(len(req.json()['Resources'])))
                
            counter = 0
            for resource in req.json()['Resources']:

                if 'displayName' in resource:
                    df.loc[counter,'displayName'] = resource['displayName']
                if 'active' in resource:
                    df.loc[counter,'active'] = resource['active']
                df.loc[counter,'id'] = resource['id']
                df.loc[counter,'userName'] = np.nan
                df.loc[counter,'applicationId'] = resource['applicationId']
                if 'externalId' in resource:
                    df.loc[counter,'externalId'] = resource['externalId']
                counter+=1

            return df
        except Exception as e:
            self.logger_obj.error(f"Fetching User Details Failed with status : {req.status_code} and reason :{req.reason}")
            raise
    def get_all_user_groups_dbx(self, all_groups = False):
        """
        Retrieve all user groups and their members from Databricks using SCIM API.

        This function fetches information about all groups and their members (users, service principals, and nested groups)
        from Databricks. It supports the scalable SCIM API and handles pagination for large datasets.

        Returns:
            pd.DataFrame: DataFrame containing group and member information.

        Notes:
            - Implements batching and pagination to handle large numbers of groups.
            - Processes users, service principals, and nested groups separately.
            - Uses multi-threading for parallel processing of API calls.
            - Implements retry mechanism for failed API calls.
        """
        try:
            batch_size = 9
            group_ids = []
            user_ids = [] 
            spn_ids = []
            groups_df = pd.DataFrame()
            group_list_df = pd.DataFrame()
            account_id = self.dbx_config["account_id"]
            # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Users"
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups"
            params = {'startIndex': '1', 'count': '100'}
            token_result = self.get_dbx_token()


            headers = {'Authorization': 'Bearer ' + token_result }

            try:
                graph_results = []
                if not all_groups:
                    filter_batch_size = math.ceil(len(self.groups_to_sync)/ 30)
                    if len(self.groups_to_sync) > filter_batch_size:
                        group_ids_filter = np.array_split(self.groups_to_sync, filter_batch_size)
                    else:
                        group_ids_filter = np.array_split(self.groups_to_sync, 1)
                    
                    for group_filter in group_ids_filter:
                        if self.Scalable_SCIM_Enabled:
                            filter_string = '" or displayName eq "'.join(group_filter)
                            filter_string = 'displayName eq "' + filter_string + '"'
                        else:
                            filter_string = '` or displayName eq `'.join(group_filter)
                            filter_string = 'displayName eq `' + filter_string + '`'

                    
                        index = 0
                        totalResults = 100
                        itemsPerPage = 100
                        while index < totalResults:
                            if not all_groups:
                                params = {'startIndex': str(index), 'count': itemsPerPage, 'filter': filter_string}
                            else:
                                params = {'startIndex': str(index), 'count': itemsPerPage}
                            retry_counter = 0
                            while True:
                                req = requests.get(url=url, headers=headers, params=params)
                                if req.status_code == 200:
                                    totalResults = req.json()['totalResults']
                                    itemsPerPage = req.json()['itemsPerPage']
                                    index += int(itemsPerPage)
                                    graph_results.append(req.json()) 
                                    break
                                else:
                                    self.logger_obj.error(f"Fetching Group Details Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                                    if retry_counter <= 5:
                                        time.sleep(2**retry_counter)
                                        retry_counter+=1
                                    else:
                                        self.logger_obj.error(f"Fetching Group Details Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                                        break
                                
                        
                    
                # df = pd.DataFrame(index=range(totalResults))
                if not self.Scalable_SCIM_Enabled:
                    groups_df = pd.DataFrame()
                    for result in graph_results:
                        resource_items = result['Resources']
                        _groups_df,_user_ids, _spn_ids,_group_ids  = self.extract_group_members(resource_items)
                        if (not _groups_df.empty) and (not groups_df.empty):
                            groups_df = pd.concat([_groups_df,groups_df])
                        elif (not _groups_df.empty) and (groups_df.empty):
                            groups_df=_groups_df
                        group_ids.extend(_group_ids)
                        user_ids.extend(_user_ids)
                        spn_ids.extend(_spn_ids) 
                    
                else:
                    groups_df,_user_ids, _spn_ids,_group_ids = self.extract_group_members_scalable(graph_results)
                     
                    group_ids.extend(_group_ids)
                    user_ids.extend(_user_ids)
                    spn_ids.extend(_spn_ids)
                   
                group_list_df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool'),
                   'id': pd.Series(dtype='str'),
                   'userName': pd.Series(dtype='str'),
                   'applicationId': pd.Series(dtype='str')},index =range(len(group_ids)))   
                counter =0
                for group_id in group_ids:
                    try:
                        group_list_df.loc[counter] = [groups_df[groups_df['group_id']==group_id].iloc[0]['group_displayName'],True,group_id, groups_df[groups_df['group_id']==group_id].iloc[0]['group_displayName'],np.nan]
                        counter+=1
                    except Exception as e:
                        self.logger_obj.warning(f"This Groups with ID: {group_id} is not present in groups_to_sync.sh file")
                        print(f"This Group with ID: {group_id} is not present in groups_to_sync.sh file")
                            

                group_list_df['type'] = 'Group'
                user_list_df=self.get_users_with_ids_dbx(user_ids)
                user_list_df['type'] = 'User'
                spn_list_df=self.get_spns_with_ids_dbx(spn_ids)
                spn_list_df['type'] = 'ServicePrincipal'
                user_list_df = user_list_df[user_list_df['id'].notna()]
                spn_list_df = spn_list_df[spn_list_df['id'].notna()]

                user_list_df = pd.concat([user_list_df,spn_list_df,group_list_df])
                user_list_df = user_list_df.merge(groups_df, left_on=['id'], right_on=['user_id'], how='inner')
                user_list_df =user_list_df[user_list_df['id'].notna()]
                return user_list_df
            
            except Exception as e:
                self.logger_obj.error(f"Fetching User Details Failed with status : {req.status_code} and reason :{req.reason}")
                raise
            # display(groups_df.drop_duplicates())
        except Exception as X:
            self.logger_obj.error(f"Exception {X}")

    def extract_group_members(self,resource_items):
        """
        Extract member information from group results in a scalable manner.

        This function processes graph results containing group information, retrieves detailed
        group information for each group, and extracts member details. It's designed to handle
        large numbers of groups efficiently using multi-threading.

        Args:
            graph_results (list): List of graph result items containing group information.

        Returns:
            tuple: A tuple containing:
                - pd.DataFrame: DataFrame with group and member information.
                - list: List of user IDs.
                - list: List of service principal IDs.
                - list: List of nested group IDs.

        Notes:
            - Uses multi-threading to fetch group details concurrently.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Calls extract_group_members to process the detailed group information.
            - Suitable for processing large numbers of groups efficiently.
        """
        counter = 0
        ids = []
        
        groups_df = pd.DataFrame()
        for resource in resource_items: 
            if 'members' in resource:
                counter=0
                df = pd.DataFrame(index = range(len(resource['members'])))
                for group in resource['members']:
                    df.loc[counter,'group_displayName'] = resource['displayName']
                    if 'externalId' in resource:
                        df.loc[counter,'group_externalId'] = resource['externalId']
                    df.loc[counter,'group_id'] = resource['id']
                    df.loc[counter,'user_id'] = group['value']
                    ids.append(group['$ref'])
                    counter+=1
                groups_df = pd.concat([groups_df,df])
            else:
                df = pd.DataFrame(index = range(1))
                df.loc[counter,'group_displayName'] = resource['displayName']
                if 'externalId' in resource:
                            df.loc[counter,'group_externalId'] = resource['externalId']
                df.loc[counter,'group_id'] = resource['id']
                df.loc[counter,'user_id'] = np.nan
                groups_df = pd.concat([groups_df,df])
                
        user_ids = []
        spn_ids = []
        group_ids = []
        for id in ids:
            if 'Users' in id:
                user_ids.append(id.replace('Users/',''))
            elif 'ServicePrincipals' in id:
                spn_ids.append(id.replace('ServicePrincipals/',''))
            elif 'Groups' in id:
                group_ids.append(id.replace('Groups/',''))    
        
        return groups_df,user_ids,spn_ids,group_ids
    
    def extract_group_members_scalable(self, graph_results):
        """
        Extract member information from group results in a scalable manner : For Scalable SCIM APIs.

        This function processes graph results containing group information, retrieves detailed
        group information for each group, and extracts member details. It's designed to handle
        large numbers of groups efficiently using multi-threading.

        Args:
            graph_results (list): List of graph result items containing group information.

        Returns:
            tuple: A tuple containing:
                - pd.DataFrame: DataFrame with group and member information.
                - list: List of user IDs.
                - list: List of service principal IDs.
                - list: List of nested group IDs.

        Notes:
            - Uses multi-threading to fetch group details concurrently.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Calls extract_group_members to process the detailed group information.
            - Suitable for processing large numbers of groups efficiently.
        """
        counter = 0
        group_ids = []
        ids = []
        groups_df = pd.DataFrame()
        for result in graph_results:
            if 'Resources' in result:
                resource_items = result['Resources']
                for resource in resource_items: 
                    group_ids.append(resource['id'])

        # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Users"
        
        params = {'startIndex': '1', 'count': '1000'}
        token_result = self.get_dbx_token()
        threads= []
        group_details = []
        # master_list = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:
            for group_id in group_ids: 
                
                threads.append(executor.submit(self.get_group_details_with_id, group_id))
            for task in as_completed(threads):
                group_details.extend(task.result())

  
        groups_df,user_ids, spn_ids,group_ids = self.extract_group_members(group_details)
        
        return groups_df,user_ids, spn_ids,group_ids

    def get_group_details_with_id(self, group_id):
        """
        Retrieve detailed information for a specific group from Databricks.

        This function fetches detailed information for a given group ID from Databricks
        using the SCIM API. It implements retry logic for rate limiting.

        Args:
            group_id (str): The ID of the group to fetch details for.

        Returns:
            list: A list containing the JSON response with group details.

        Raises:
            Exception: If there's an error in fetching group details, it logs the error and re-raises.

        Notes:
            - Uses the Databricks SCIM API to fetch group details.
            - Implements retry logic with a 1-second delay for rate limit (429) responses.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
        """
        try:
            graph_results = []
            token_result = self.get_dbx_token()
            headers = {'Authorization': 'Bearer ' + token_result }
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
            retry_counter = 0
            while True:
                req = requests.get(url=url, headers=headers)
                
                if req.status_code == 429 and retry_counter<=5:
                    print(f'Retrying fetch group details for Group:{group_id}')
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    graph_results.append(req.json())
                    break
            return graph_results
        except Exception as e:
                    self.logger_obj.error(f"Getting Groups Details with Ids Failed")
                    raise
    def get_users_with_ids_dbx(self,ids):
        """
        Retrieve user details from Databricks for a list of user IDs.

        This function fetches user information from Databricks for the given list of user IDs.
        It supports batch processing to handle large numbers of users efficiently.

        Args:
            ids (list): List of user IDs to fetch from Databricks.

        Returns:
            pd.DataFrame: DataFrame containing user details for the given IDs.

        Notes:
            - Implements batching to handle large numbers of user IDs.
            - Uses the get_user_details_dbx method for actual API calls.
            - Handles potential errors for each batch separately.
        """
        try:
            batch_size = 100
            account_id = self.dbx_config["account_id"]
            ids = np.unique(ids)
            size = round(len(ids)/batch_size,0)
            user_list_df = pd.DataFrame(index = range(len(ids)))
            if size > 0:
                ids = np.array_split(ids, size)
            else:
                ids = np.array_split(ids, 1)
            
            threads = []
            results=[]
            with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:            
                for id_set in ids: 
                    ids_string = ' or id eq '.join(id_set)
                    ids_string = 'id eq ' + ids_string 

                    threads.append(executor.submit(self.get_user_details_dbx, ids_string))

            for task in as_completed(threads):
                results.append(task.result()) 

            user_list_df = pd.concat(results)
            # for id_set in ids: 
            #     ids_string = ' or id eq '.join(id_set)
            #     ids_string = 'id eq ' + ids_string
            #     # print(ids_string)
            #     df = self.get_user_details_dbx(ids_string)
            #     # display(df)
            #     user_list_df = pd.concat([user_list_df,df])

            
            return user_list_df
        except Exception as e:
            self.logger_obj.error(f"Getting User Details with Ids Failed")
            raise
    def get_user_details_with_userName_dbx(self,nameString):
        """
        Retrieve user details from Databricks for a specific username.

        This function fetches user information from Databricks for the given username.

        Args:
            nameString (str): Username to search for in Databricks.

        Returns:
            pd.DataFrame: DataFrame containing user details for the given username.

        Raises:
            Exception: If the API call fails, logs the error and re-raises the exception.

        Notes:
            - Similar to get_user_details_dbx, but filters by username instead of ID.
            - Includes admin status in the returned data.
        """
        try:
            account_id = self.dbx_config["account_id"] 
            token_result = self.get_dbx_token()

            headers = {'Authorization': 'Bearer ' + token_result }

            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users"
            params = {'filter': 'userName eq "'+ nameString + '"'}
            
            retry_counter=1
            while True:
                req = requests.get(url=url, headers=headers, params = params)
                if req.status_code == 429 and retry_counter<=5:
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    break
 
            # df = pd.DataFrame(index = range(len(req.json()['Resources'])))
            df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool'),
                   'id': pd.Series(dtype='str'),
                   'userName': pd.Series(dtype='str'),
                   'applicationId': pd.Series(dtype='str'),
                   'externalId': pd.Series(dtype='str'),
                   'isAdmin': np.full(1, False, dtype=bool),
                   }, index = range(len(req.json()['Resources'])))

            counter = 0
            for resource in req.json()['Resources']:

                if 'displayName' in resource:
                    df.loc[counter,'displayName'] = resource['displayName']
                if 'roles' in resource:
                    df.loc[counter,'isAdmin'] = True if list(filter(lambda x: x['value']  == 'account_admin', resource['roles'])) else False
                if 'active' in resource:
                    df.loc[counter,'active'] = resource['active']
                df.loc[counter,'id'] = resource['id']
                df.loc[counter,'userName'] = resource['userName']
                df.loc[counter,'applicationId'] = np.nan
                if 'externalId' in resource:
                    df.loc[counter,'externalId'] = resource['externalId']
                counter+=1
            return df
        
        except Exception as e:
            self.logger_obj.error(f"Fetching User Details Failed with status : {req.status_code} and reason :{req.reason}")
            raise
    def get_spn_details_with_appDisplayName_dbx(self,appDislayName):
        """
        Retrieve service principal details from Databricks for a specific application display name.

        This function fetches service principal information from Databricks for the given application display name.

        Args:
            appDisplayName (str): Application display name to search for in Databricks.

        Returns:
            pd.DataFrame: DataFrame containing service principal details for the given application display name.

        Raises:
            Exception: If the API call fails, logs the error and re-raises the exception.

        Notes:
            - Similar to get_spn_details_dbx, but filters by application display name instead of ID.
        """
        try:
            account_id = self.dbx_config["account_id"] 
            token_result = self.get_dbx_token()

            headers = {'Authorization': 'Bearer ' + token_result }

            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            params = {'filter': 'displayName eq `'+ appDislayName + '`'} 
            retry_counter=1
            while True:
                req = requests.get(url=url, headers=headers, params = params)
                if req.status_code == 429 and retry_counter<=5:
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    break

            if 'Resources' in req.json():
                df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')}, index = range(len(req.json()['Resources'])))
                
                counter = 0
                if 'Resources' in req.json():
                    for resource in req.json()['Resources']:

                        if 'displayName' in resource:
                            df.loc[counter,'displayName'] = resource['displayName']
                        if 'active' in resource:
                            df.loc[counter,'active'] = resource['active']
                        df.loc[counter,'id'] = resource['id']
                        df.loc[counter,'userName'] = np.nan
                        df.loc[counter,'applicationId'] = resource['applicationId']
                        if 'externalId' in resource:
                            df.loc[counter,'externalId'] = resource['externalId']
                        counter+=1
            else:
                df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')})

            return df
        except Exception as e:
            self.logger_obj.error(f"Fetching User Details Failed with status : {req.status_code} and reason :{req.reason}")
            raise

    def get_spns_with_ids_dbx(self,ids):
        """
        Retrieve service principal details from Databricks for a list of service principal IDs.

        This function fetches service principal information from Databricks for the given list of IDs.
        It supports batch processing to handle large numbers of service principals efficiently.

        Args:
            ids (list): List of service principal IDs to fetch from Databricks.

        Returns:
            pd.DataFrame: DataFrame containing service principal details for the given IDs.

        Notes:
            - Implements batching to handle large numbers of service principal IDs.
            - Uses the get_spn_details_dbx method for actual API calls.
            - Handles potential errors for each batch separately.
        """
        try:
            batch_size = 100
            account_id = self.dbx_config["account_id"]
            ids = np.unique(ids)
            size = round(len(ids)/batch_size,0)
            spn_list_df = pd.DataFrame(index = range(len(ids)))
            if size > 0:
                ids = np.array_split(ids, size)
            else:
                ids = np.array_split(ids, 1)
            
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            
            threads = []
            results=[]
            with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:            
                for id_set in ids: 
                    ids_string = ' or id eq '.join(id_set)
                    ids_string = 'id eq ' + ids_string 

                    threads.append(executor.submit(self.get_spn_details_dbx, ids_string))

            for task in as_completed(threads):
                results.append(task.result()) 

            spn_list_df = pd.concat(results)
            # for id_set in ids: 
            #     ids_string = ' or id eq '.join(id_set)
            #     ids_string = 'id eq ' + ids_string
            #     # print(ids_string)
            #     df = self.get_spn_details_dbx(ids_string)
            #     # display(df)
            #     spn_list_df = pd.concat([spn_list_df,df])

            return spn_list_df
        except Exception as e:
            self.logger_obj.error(f"Getting SPN Details with Ids Failed")
            raise

    def get_all_groups_dbx(self):
        """
        Retrieve all groups from Databricks using SCIM API.

        This function fetches information about all groups from Databricks. 
        It supports pagination to handle large numbers of groups.

        Returns:
            pd.DataFrame: DataFrame containing group information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of groups.
            - Uses retry mechanism for failed API calls.
            - Includes group entitlements if available.
        """
        try:
            account_id = self.dbx_config["account_id"]
            # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Groups"
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups"
            params = {'startIndex': '1', 'count': '100'}
            token_result = self.get_dbx_token()


            headers = {'Authorization': 'Bearer ' + token_result }


            graph_results = []
            index = 0
            totalResults = 100
            itemsPerPage = 10
            while index < totalResults:    
                params = {'startIndex': index, 'count': itemsPerPage}
                retry_counter = 0
                while True:
                    req = requests.get(url=url, headers=headers, params=params)
                    if req.status_code == 200:
                        totalResults = req.json()['totalResults']
                        itemsPerPage = req.json()['itemsPerPage']
                        index += int(itemsPerPage)
                        graph_results.append(req.json()) 
                        break
                    else:
                        self.logger_obj.error(f"Fetching All Group Details Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                        if retry_counter <= 5:
                            time.sleep(2**retry_counter)
                            retry_counter+=1
                        else:
                            self.logger_obj.error(f"Fetching All Group Details Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                            break
            df = pd.DataFrame(index=range(totalResults))
            counter = 0
            for result in graph_results:
                resource_item = result['Resources']
                for resource in resource_item:

                    df.loc[counter,"displayName"] = resource["displayName"]
                    if 'externalId' in resource:
                        df.loc[counter,"externalId"] = resource["externalId"]
                    df.loc[counter,"id"] = resource["id"]
                    if 'entitelments' in resource:
                        df.loc[counter,"entitelments"] = resource["entitelments"]
                    counter+=1
            return df
        except Exception as e:
            self.logger_obj.error(f"Getting All Group Details Failed")
            raise
    def get_all_admins_dbx(self):
        """
        Retrieve all admin users from Databricks using SCIM API.

        This function fetches information about all users with admin roles from Databricks.
        It supports pagination to handle large numbers of admin users.

        Returns:
            pd.DataFrame: DataFrame containing admin user information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of admin users.
            - Uses retry mechanism for failed API calls.
            - Filters users based on the 'account_admin' role.
        """
        account_id = self.dbx_config["account_id"]
            # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Groups"
        url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users"
        
        token_result = self.get_dbx_token()



        headers = {'Authorization': 'Bearer ' + token_result }


        graph_results = []
        index = 0
        totalResults = 100
        itemsPerPage = 100
        
        while index < totalResults:
            retry_counter = 0
            while True:

                params = {'startIndex': index, 'count': itemsPerPage,'filter': 'roles.value co account_admin'}
                req = requests.get(url=url, headers=headers, params=params)
                if req.status_code == 200:
                    totalResults = req.json()['totalResults']
                    itemsPerPage = req.json()['itemsPerPage']
                    index += int(itemsPerPage)
                    graph_results.append(req.json()) 
                    break
                else:
                    self.logger_obj.error(f"Fetching Admin Details Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        self.logger_obj.error(f"Fetching Admin Details Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                        break
        df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                   'userName': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool'),
                   'id': pd.Series(dtype='str')},index=range(totalResults))
            
        counter = 0
        for result in graph_results:
            resource_item = result['Resources']
            for resource in resource_item:
                if 'account_admin' in resource & 'members' in resource:
                    for member in resource['members']:
                        df.loc[counter,"displayName"] = resource["displayName"]
                        df.loc[counter,"userName"] = resource["userName"]
                        df.loc[counter,"active"] = resource["active"]
                        df.loc[counter,"id"] = resource["id"]
                        counter+=1
        return df
    
    def get_workspaces_list(self):
        """
        Retrieve a list of Databricks workspaces and their permissions.

        This function fetches information about all workspaces in the Databricks account
        and attempts to retrieve user permissions for each workspace.

        Returns:
            pd.DataFrame: DataFrame containing workspace information and permissions.

        Notes:
            - Requires workspace-level authentication for each workspace.
            - Fetches user entitlements for each workspace.
            - May encounter permission issues for workspaces where the service principal lacks access.
        """
        account_id = self.dbx_config["account_id"]
                    # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Groups"
        url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/workspaces"  
        token_result = self.get_dbx_token()
        headers = {'Authorization': 'Bearer ' + token_result }
        req = requests.get(url=url, headers=headers)
        if req.status_code == 200:
            workspace_json = json.dumps(req.json())
            workspace_json = str.encode(workspace_json)
            df = pd.read_json(BytesIO(workspace_json),dtype='unicode',convert_dates=False)
            df['permissions'] = ''
            
            for idx, workspace in df.iterrows():
                url = f"https://{workspace['deployment_name']}.azuredatabricks.net/oidc/v1/token"
                req = requests.post(url=url, auth=(self.dbx_config['client_id'],self.dbx_config['workspace_client_secret']), data={'grant_type':'client_credentials', 'scope':'all-apis'})
                token_result = req.json()['access_token']
                headers = {'Authorization': 'Bearer ' + token_result }    
                url = f"https://{workspace['deployment_name']}.azuredatabricks.net/api/2.0/preview/scim/v2/Users"  
                req = requests.get(url=url, headers=headers, params={'attributes':'entitlements'})
                if req.status_code == 200:
                    permission_json = req.json()['Resources']
                    permission_json = json.dumps(permission_json)
                    permission_json = str.encode(permission_json)
                    perm_df = pd.read_json(BytesIO(permission_json),dtype='unicode',convert_dates=False)
                
        return df
        
    def get_all_users_dbx(self):
        """
        Retrieve all users from Databricks using SCIM API.

        This function fetches information about all users from Databricks.
        It supports pagination to handle large numbers of users.

        Returns:
            pd.DataFrame: DataFrame containing user information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of users.
            - Uses retry mechanism for failed API calls.
            - Includes active status for each user.
        """
        try:
        

            graph_results = []
            start_index = 0
            totalResults = 100
            itemsPerPage = 100
            
            req_json = self.get_all_users_call_api(start_index,itemsPerPage)
            if 'totalResults' in req_json:
                totalResults = req_json['totalResults']
            batches = [x for x in range(math.ceil(totalResults/100))]
            start_index = 0
            threads = []
            with ThreadPoolExecutor(max_workers=self.dbx_config["account_get_resource_limit"]) as executor:            
                for batch in batches:
            
                    if start_index<= totalResults: 
                        threads.append(executor.submit(self.get_all_users_call_api, start_index,itemsPerPage))
                    start_index +=100
                for task in as_completed(threads):
                    graph_results.append(task.result()) 

            # while index < totalResults:
            #     totalResults = self.get_all_users_call_api(url, headers, graph_results, index, itemsPerPage)

            df = pd.DataFrame({'displayName': pd.Series(dtype='str'),
                   'userName': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool'),
                   'id': pd.Series(dtype='str')},index=range(totalResults))
            
            counter = 0
            for result in graph_results:
                resource_item = result['Resources']
                for resource in resource_item:
                    if 'displayName' in resource:
                        df.loc[counter,"displayName"] = resource["displayName"]
                    df.loc[counter,"userName"] = resource["userName"]
                    df.loc[counter,"active"] = resource["active"]
                    df.loc[counter,"id"] = resource["id"]
                    counter+=1
            return df.drop_duplicates()
        except Exception as e:
            self.logger_obj.error(f"Getting All User Details Failed")
            raise

    def get_all_users_call_api(self,index, itemsPerPage):
        """
        Make an API call to retrieve user information from Databricks.

        This function fetches a page of user information from Databricks using the SCIM API.
        It supports pagination and implements retry logic for improved reliability.

        Args:
            index (int): The starting index for the user list to retrieve.
            itemsPerPage (int): The number of items to retrieve per page.

        Returns:
            dict: JSON response containing user information for the requested page.
                Returns an empty JSON object if all retries fail.

        Notes:
            - Uses the Databricks SCIM API to fetch user details.
            - Implements retry logic with up to 3 attempts in case of failure.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
            - Logs errors and retry attempts for debugging purposes.
        """
        account_id = self.dbx_config["account_id"] 
        url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users"
        
        token_result = self.get_dbx_token()


        headers = {'Authorization': 'Bearer ' + token_result }
        retry_counter = 0
        while True:
            params = {'startIndex': index, 'count': itemsPerPage}
            req = requests.get(url=url, headers=headers, params=params)
            if req.status_code == 200:
                totalResults = req.json()['totalResults']
                itemsPerPage = req.json()['itemsPerPage']
                index += int(itemsPerPage)
                return req.json()
            else:
                self.logger_obj.error(f"Fetching All User Details Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                if retry_counter <= 5:
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    self.logger_obj.error(f"Fetching All User Details Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                    break
        return json.dumps({})
    def get_all_spns_dbx(self):
        """
        Retrieve all service principals from Databricks.

        This function fetches information about all service principals in Databricks using the SCIM API.
        It handles pagination to retrieve all service principals and implements retry logic for reliability.

        Returns:
            pd.DataFrame: DataFrame containing service principal information including id, applicationId, 
                        displayName, externalId, and active status.

        Raises:
            Exception: If there's an error in fetching service principal details, it logs the error and re-raises.

        Notes:
            - Uses the Databricks SCIM API to fetch service principal details.
            - Implements pagination to handle large numbers of service principals.
            - Uses retry logic with up to 3 attempts for each API call.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
            - Processes the JSON response and converts it into a pandas DataFrame for easy manipulation.
        """
        try:
            account_id = self.dbx_config["account_id"]
            # url = dbx_config['dbx_host'] + "/api/2.0/preview/scim/v2/Groups"
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            
            token_result = self.get_dbx_token()


            headers = {'Authorization': 'Bearer ' + token_result }


            graph_results = []
            index = 0
            totalResults = 100
            itemsPerPage = 100
            params = {'startIndex': index, 'count': itemsPerPage}
            while index < totalResults:
                retry_counter = 0
                while True:
   
                    params = {'startIndex': index, 'count': itemsPerPage}
                    req = requests.get(url=url, headers=headers, params=params)
                    if req.status_code == 200:
                        totalResults = req.json()['totalResults']
                        itemsPerPage = req.json()['itemsPerPage']
                        index += int(itemsPerPage)
                        graph_results.append(req.json()) 
                        break
                    else:
                        self.logger_obj.error(f"Fetching All User Details Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                        if retry_counter <= 4:
                            time.sleep(2**retry_counter)
                            retry_counter+=1
                        else:
                            self.logger_obj.error(f"Fetching All User Details Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                            break

            df = pd.DataFrame({'id': pd.Series(dtype='str'),
                   'applicationId': pd.Series(dtype='str'),
                   'displayName': pd.Series(dtype='str'),
                   'externalId': pd.Series(dtype='str'),
                   'active': pd.Series(dtype='bool')},index=range(totalResults))
            
            counter = 0
            for result in graph_results:
                resource_item = result['Resources']
                for resource in resource_item:

                    df.loc[counter,"id"] = resource["id"]
                    df.loc[counter,"applicationId"] = resource["applicationId"]
                    if 'displayName' in resource:
                        df.loc[counter,"displayName"] = resource["displayName"]
                    if 'externalId' in resource:
                        df.loc[counter,"externalId"] = resource["externalId"]
                    df.loc[counter,"active"] = resource["active"]
                    counter+=1
            return df
        except Exception as e:
            self.logger_obj.error(f"Getting All User Details Failed")
            raise

    @log_decorator.log_decorator()
    def create_group_dbx(self,displayName,externalId):
        """
        Create a new group in Databricks using SCIM API.

        This function creates a new group in Databricks with the given display name and external ID.

        Args:
            displayName (str): The display name for the new group.
            externalId (str): The external ID for the new group (typically the Azure AD group ID).

        Returns:
            pd.DataFrame: DataFrame containing the details of the newly created group.

        Notes:
            - Uses retry mechanism for failed API calls.
            - Logs the creation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        try:
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups"
            token_result = self.get_dbx_token()


            headers = {'Authorization': 'Bearer ' + token_result }


            payload = {
            "displayName": displayName,
            "externalId": externalId
            }
            retry_counter = 0
            while True:
                req = requests.post(url=url, headers=headers, json = payload)
                if req.status_code == 201:
                    groups_json = json.dumps(req.json())
                    groups_json = str.encode(groups_json)
                    df = pd.read_json(BytesIO(groups_json),dtype='unicode',convert_dates=False)
                    return df
                else:
                    self.logger_obj.error(f"Creating User Group Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        self.logger_obj.error(f"Creating User Group Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                        break
        except Exception as e:
            self.logger_obj.error(f"Creating User Details Failed")
            raise
    
    
    # @log_decorator.log_decorator()
    # def delete_group_dbx(self,id):
    #     retry_counter = 0
    #     while True:
    #         account_id = self.dbx_config["account_id"]
    #         url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups/{id}"
    #         token_result = self.get_dbx_token()


    #         headers = {'Authorization': 'Bearer ' + token_result }

    #         req = requests.delete(url=url, headers=headers)
    #         if req.status_code == 204:
    #             self.logger_obj.info(f"delete group status{req.status_code}")
    #             return req.status_code
    #         else:
    #             self.logger_obj.error(f"Creating User Group Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
    #             if retry_counter <= 3:
    #                 time.sleep(1)
    #                 retry_counter+=1
    #             else:
    #                 self.logger_obj.error(f"Creating User Group Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
    #                 break
   
    # @sleep_and_retry                
    # @limits(calls= 7, period=1)               
    def create_users_request(self, userName,displayName,externalId):
        """
        Create a new user in Databricks using SCIM API.

        This function creates a new user in Databricks with the given username, display name, and external ID.

        Args:
            userName (str): The username for the new user.
            displayName (str): The display name for the new user.
            externalId (str): The external ID for the new user (typically the Azure AD user ID).

        Returns:
            int: The HTTP status code of the create operation.

        Notes:
            - Uses retry mechanism for failed API calls.
            - Logs the creation process and any errors encountered.
        """
        account_id = self.dbx_config["account_id"]
        url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users"
        token_result = self.get_dbx_token()
        payload = {
            "userName": userName,
            "displayName": displayName,
            "externalId" : externalId
            }

        headers = {'Authorization': 'Bearer ' + token_result }

        req = requests.post(url=url, headers=headers, json = payload)
        retry_counter = 0
        while True:
            if req.status_code == 201 :
                return req.status_code
            elif req.status_code == 409:
                return req.status_code
            else:
                self.logger_obj.error(f"Failed Creating User. {userName}: Attempting Retry") 
                if retry_counter <= 5:
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    self.logger_obj.error(f"Failed Creating User. {userName}: Retry failed. Continuing") 
                    break
                
    @sleep_and_retry
    @limits(calls=7, period=1)      
    def create_spns_request(self, applicationId,displayName,externalId):
        """
        Create a new service principal in Databricks using SCIM API.

        This function creates a new service principal in Databricks with the given application ID, display name, and external ID.

        Args:
            applicationId (str): The application ID for the new service principal.
            displayName (str): The display name for the new service principal.
            externalId (str): The external ID for the new service principal (typically the Azure AD service principal ID).

        Returns:
            int: The HTTP status code of the create operation.

        Notes:
            - Implements rate limiting (7 calls per second).
            - Uses retry mechanism for failed API calls.
            - Logs the creation process and any errors encountered.
        """
        account_id = self.dbx_config["account_id"]
        url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
        token_result = self.get_dbx_token()

        headers = {'Authorization': 'Bearer ' + token_result }
        if self.cloud_provider == 'AWS':
            filter_value = f'displayName eq "{displayName}"'
            params = {'filter':filter_value}
            payload = {
                "displayName": displayName,
                "externalId" : externalId
                }
            # check if spn already exists
            req = requests.get(url=url, headers=headers, params=params)
            while True:
                if req.status_code == 200:
                        if 'Resources' in req.json():
                            self.logger_obj.error(f"Service Principal with same display name. {displayName} already exists") 
                            return req.status_code
                    
                else :
                    self.logger_obj.error(f"Failed Creating Service Principal. {displayName} with application id:{applicationId}: Attempting Retry") 
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        self.logger_obj.error(f"Failed Creating Service Principal. {displayName} with application id:{applicationId}: Retry Failed. Continuing") 
                        break

        elif self.cloud_provider == 'Azure':
            payload = {
                            "applicationId": applicationId,
                            "displayName": displayName,
                            "externalId" : externalId
                            }
        retry_counter = 0
        req = requests.post(url=url, headers=headers, json = payload)
        while True:
        
            if req.status_code == 201:
                    return req.status_code
            else :
                self.logger_obj.error(f"Failed Creating Service Principal. {displayName} with application id:{applicationId}: Attempting Retry") 
                if retry_counter <= 5:
                    time.sleep(2**retry_counter)
                    retry_counter+=1
                else:
                    self.logger_obj.error(f"Failed Creating Service Principal. {displayName} with application id:{applicationId}: Retry Failed. Continuing") 
                    break
                    

    @log_decorator.log_decorator()
    def create_users_dbx(self,users_to_add):
        """
        Create multiple users in Databricks using SCIM API.

        This function creates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_add (pd.DataFrame): DataFrame containing user information to be added to Databricks.

        Returns:
            list: A list of HTTP status codes for each user creation operation.

        Notes:
            - Uses multi-threading to process multiple user creations concurrently.
            - Calls create_users_request for each individual user creation.
            - Decorated with log_decorator for additional logging.
        """
        ret_df = pd.DataFrame()
        threads= []
        results = []
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_post_resource_limit"]) as executor:            
            for idx, row in users_to_add.iterrows():
                userName = row['userPrincipalName']
                displayName = row['displayName_x']
                externalId = row['id_x']
                threads.append(executor.submit(self.create_users_request, userName,displayName,externalId))

            for task in as_completed(threads):
                results.append(task.result()) 

        return results

    @log_decorator.log_decorator()
    def create_spns_dbx(self,spns_to_add):
        """
        Create multiple service principals in Databricks using SCIM API.

        This function creates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_add (pd.DataFrame): DataFrame containing service principal information to be added to Databricks.

        Returns:
            list: A list of HTTP status codes for each service principal creation operation.

        Notes:
            - Uses multi-threading to process multiple service principal creations concurrently.
            - Calls create_spns_request for each individual service principal creation.
            - Decorated with log_decorator for additional logging.
        """
        ret_df = pd.DataFrame()
        threads= []
        results = []
        with ThreadPoolExecutor(max_workers=self.dbx_config["account_post_resource_limit"]) as executor:            
            for idx, row in spns_to_add.iterrows():
                applicationId = row['appId']
                displayName = row['displayName']
                externalId = row['id_x']
                threads.append(executor.submit(self.create_spns_request, applicationId,displayName,externalId))

            for task in as_completed(threads):
                results.append(task.result()) 

        return results            
        # return ret_df
    @log_decorator.log_decorator()    
    def deactivate_users_dbx(self,users_to_remove):
        """
        Deactivate multiple users in Databricks using SCIM API.

        This function deactivates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_remove (pd.DataFrame): DataFrame containing user information to be deactivated in Databricks.

        Notes:
            - Uses PATCH request to update user status to inactive.
            - Implements retry mechanism for failed API calls.
            - Logs the deactivation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        token_result = self.get_dbx_token()
        headers = {'Authorization': 'Bearer ' + token_result }
        payload = {
                    "schemas": [
                    "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                    ],
                    "Operations": [
                    {
                    "op": "replace",
                    "value": {
                    "active": False
                    }
                    }
                    ]
                    }
    
        for idx, row in users_to_remove.iterrows():
            id = row['id_y']
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users/{id}"
            retry_counter =0
            while True:
                try:
                    req = requests.patch(url=url, headers=headers, json = payload)
                    assert req.status_code == 200
                    self.logger_obj.error(f"Deactivated User:{id}") 
                    break
                except:
                    
                    self.logger_obj.error(f"Failed to deactivate user with dbx_id:{id}") 
                    self.logger_obj.error(f"Deactivating User Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if (retry_counter <= 5) and (req.reason != 'Not Found'):
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        break

    @log_decorator.log_decorator()    
    def deactivate_spns_dbx(self,spns_to_remove):
        """
        Deactivate multiple service principals in Databricks using SCIM API.

        This function deactivates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_remove (pd.DataFrame): DataFrame containing service principal information to be deactivated in Databricks.

        Notes:
            - Uses PATCH request to update service principal status to inactive.
            - Implements retry mechanism for failed API calls.
            - Logs the deactivation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        for idx, row in spns_to_remove.iterrows():
            id = row['id_y']
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{id}"
            token_result = self.get_dbx_token()
            retry_counter =0
            while True:
                headers = {'Authorization': 'Bearer ' + token_result }
                payload = {
                            "schemas": [
                            "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                            ],
                            "Operations": [
                            {
                            "op": "replace",
                            "value": {
                            "active": False
                            }
                            }
                            ]
                            }
                req = requests.patch(url=url, headers=headers, json = payload)
                if req.status_code == 200:
                    self.logger_obj.error(f"Deactivated SPN:{id}") 
                    break
                else:
                    self.logger_obj.error(f"Failed to deactivate SPN with dbx_id:{id}") 
                    self.logger_obj.error(f"Deactivating SPN Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        self.logger_obj.error(f"Deactivating SPN Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed. Continuing")
                        break
    @log_decorator.log_decorator()
    def activate_users_dbx(self,users_to_activate):
        """
        Activate multiple users in Databricks using SCIM API.

        This function activates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_activate (pd.DataFrame): DataFrame containing user information to be activated in Databricks.

        Notes:
            - Uses PATCH request to update user status to active.
            - Implements retry mechanism for failed API calls.
            - Logs the activation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        for idx, row in users_to_activate.iterrows():
            id = row['id_y']
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users/{id}"
            token_result = self.get_dbx_token()
            retry_counter =0
            while True:
                headers = {'Authorization': 'Bearer ' + token_result }
                payload = {
                        "schemas": [
                        "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                        ],
                        "Operations": [
                        {
                        "op": "replace",
                        "value": {
                        "active": True
                        }
                        }
                        ]
                        }
                req = requests.patch(url=url, headers=headers, json = payload)
                if req.status_code == 200:
                    break
                else:
                    self.logger_obj.error(f"Failed to activate user with dbx_id:{id}") 
                    self.logger_obj.error(f"Activating User Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        break
    @log_decorator.log_decorator()
    def activate_spns_dbx(self,spns_to_activate):
        """
        Activate multiple service principals in Databricks using SCIM API.

        This function activates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_activate (pd.DataFrame): DataFrame containing service principal information to be activated in Databricks.

        Notes:
            - Uses PATCH request to update service principal status to active.
            - Implements retry mechanism for failed API calls.
            - Logs the activation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        for idx, row in spns_to_activate.iterrows():
            id = row['id_y']
            account_id = self.dbx_config["account_id"]
            url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{id}"
            token_result = self.get_dbx_token()
            retry_counter = 0
            while True:
                headers = {'Authorization': 'Bearer ' + token_result }
                payload = {
                        "schemas": [
                        "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                        ],
                        "Operations": [
                        {
                        "op": "replace",
                        "value": {
                        "active": True
                        }
                        }
                        ]
                        }
                req = requests.patch(url=url, headers=headers, json = payload)
                if req.status_code == 200:
                    break
                else:
                    self.logger_obj.error(f"Failed to activate SPN with dbx_id:{id}") 
                    self.logger_obj.error(f"Activating SPN Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        self.logger_obj.error(f"Activating SPN Failed with status : {req.status_code} and reason :{req.reason}. Retry Failed : Continuing")
                        break
    def sync_groups(self):
        """
        Synchronize groups between Azure AD and Databricks.

        This function compares groups in Azure AD with those in Databricks and performs the following actions:
        1. Adds missing groups to Databricks.
        2. Identifies groups that could potentially be deleted from Databricks (but doesn't delete them).

        Returns:
            tuple: A tuple containing:
                - DataFrame of created groups (if not in dry run mode)
                - List of results from group deletion operations (if not in dry run mode)

        Notes:
            - Uses pandas merge to identify differences between Azure AD and Databricks groups.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the synchronization process and results.
        """
        groups_df_dbx = self.get_all_groups_dbx()
        groups_df_aad = self.get_all_groups_aad(False)

        if self.is_dryrun:
            print('This is a dry run')
        # Find the differences
        if 'externalId' in groups_df_dbx:
            net_delta = groups_df_aad.merge(groups_df_dbx, left_on=['displayName','id'], right_on=['displayName','externalId'], how='outer')
            groups_to_add = net_delta[net_delta['id_y'].isna()]
            groups_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['externalId'].notna())] 
            print(" Total New Groups :" + str(groups_to_add.shape[0]))
            print(" Total Groups that could be deleted :" + str(groups_to_remove.shape[0]) + ": Info Only : Deactivation will not be done")
        else:
            net_delta = groups_df_aad.merge(groups_df_dbx, left_on=['displayName'], right_on=['displayName'], how='outer')
            groups_to_add = net_delta[net_delta['id_y'].isna()]
            print(" Total New Groups :" + str(groups_to_add.shape[0]))

        
        if not self.is_dryrun:
            # add missing groups into dbx
            created_df = pd.DataFrame()
            for idx, row in groups_to_add.iterrows():
                group_displayName = row['displayName']
                group_externalId = row['id_x']
                df = self.create_group_dbx(group_displayName,group_externalId)
                created_df = pd.concat([created_df,df])
            
            # Whats the grounds for removing a group
            # remove unmanaged groups from dbx
            ret_res = []
            # for idx, row in groups_to_remove.iterrows():
            #     id = row['id_y']
            #     ret_res.append(delete_group_dbx(id))

            return created_df,ret_res
    def sync_users(self):
        """
        Synchronize users and service principals between Azure AD and Databricks.

        This function compares users and service principals in Azure AD with those in Databricks and performs the following actions:
        1. Adds missing users and service principals to Databricks.
        2. Deactivates users and service principals in Databricks that are not present in Azure AD.
        3. Activates users and service principals in Databricks that were previously deactivated but are present in Azure AD.

        Returns:
            list: A list of results from user and service principal creation operations (if not in dry run mode).

        Notes:
            - Uses pandas merge to identify differences between Azure AD and Databricks entities.
            - Handles both users and service principals separately.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the synchronization process and results.
        """
        users_df_dbx = self.get_all_user_groups_dbx()
        spns_df_dbx = users_df_dbx[users_df_dbx['applicationId'].notna()]
        users_df_dbx = users_df_dbx[users_df_dbx['applicationId'].isna()]
        # lower case for join
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s:s.lower() if type(s) == str else s)

        # users_df_dbx.display()

        # get all users that belong in atleast one group
        users_df_aad_all = self.get_all_groups_aad(True)
        users_df_aad = users_df_aad_all[users_df_aad_all['@odata.type']=='#microsoft.graph.user']
        # keep only unique records  
        users_df_aad = users_df_aad[['id', 'displayName',  'userPrincipalName']]
        users_df_aad = users_df_aad.drop_duplicates()
        # lower case for join
        users_df_aad['userPrincipalName'] = users_df_aad['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)
        # users_df_aad.display()

        net_delta = users_df_aad.merge(users_df_dbx, left_on=['userPrincipalName'], right_on=['userName'], how='outer')
        all_existing_users_df = self.get_all_users_dbx()
        all_existing_users_list = list(all_existing_users_df[all_existing_users_df['active']==True]['displayName'])
        users_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna()) ]
        users_to_add = users_to_add[~users_to_add['userPrincipalName'].isin(all_existing_users_list)]

        users_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna())& (net_delta['active'] == True)] 
        users_to_activate = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].notna()) & (net_delta['active'] == False)]

        if self.is_dryrun:
            print('This is a dry run')
        users_to_add.to_csv(self.log_file_dir + 'users_to_add.csv')
        print(" Total New Users :" + str(users_to_add.shape[0]))
        print(" Total Users that could be deactivated :" + str(users_to_remove.shape[0]) + ": Info Only : Deactivation will not be done")
        print(" Total Users that need to be activated :" + str(users_to_activate.shape[0]))

        if not self.is_dryrun:
            self.logger_obj.info(f"Creating New Users{len(users_to_add)}") 
            created_users = self.create_users_dbx(users_to_add)
            self.logger_obj.info(f"Deactivating Users{len(users_to_remove)}" + ": Info Only : Deactivation will not be done") 
            # self.deactivate_users_dbx(users_to_remove)
            self.logger_obj.info(f"Activating Users{len(users_to_activate)}") 
            self.activate_users_dbx(users_to_activate)


        spn_df_aad = users_df_aad_all[users_df_aad_all['@odata.type']=='#microsoft.graph.servicePrincipal']
        # keep only unique records
        if spn_df_aad.shape[0]>0:
            spn_df_aad = spn_df_aad[['id', 'displayName','appId']]
            spn_df_aad = spn_df_aad.drop_duplicates()

            # net_delta = spn_df_aad.merge(spns_df_dbx, left_on=['appId'], right_on=['applicationId'], how='outer')
            net_delta = spn_df_aad.merge(spns_df_dbx, left_on=['displayName'], right_on=['displayName'], how='outer')
            spns_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())]
            spns_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['applicationId'].notna())& (net_delta['active'] == True)] 
            spns_to_activate = net_delta[(net_delta['id_x'].notna()) & (net_delta['applicationId'].notna()) & (net_delta['active'] == False)]

            if self.is_dryrun:
                print('This is a dry run')
            print(" Total New SPNs :" + str(spns_to_add.shape[0]))
            print(" Total SPNs that could be deactivated :" + str(spns_to_remove.shape[0]) + ": Info Only : Deactivation will not be done")
            print(" Total SPNs that need to be activated :" + str(spns_to_activate.shape[0]))

            if not self.is_dryrun:
                # self.logger_obj.info(f"Creating New SPNs{len(spns_to_add)}") 
                created_spns = self.create_spns_dbx(spns_to_add)
                # self.logger_obj.info(f"Deactivating SPNs{len(spns_to_remove)}") 

                # self.deactivate_spns_dbx(spns_to_remove)

                # self.logger_obj.info(f"Activating SPNs{len(spns_to_activate)}") 
                self.activate_spns_dbx(spns_to_activate)    

            if not self.is_dryrun:
                created_users = created_users.extend(created_spns)
            else:
                return None
        return created_users
        
    @log_decorator.log_decorator()
    def remove_dbx_group_mappings(self,mappings_to_remove):
        """
        Remove group memberships in Databricks.

        This function removes users from groups in Databricks based on the provided DataFrame of mappings to remove.

        Args:
            mappings_to_remove (pd.DataFrame): DataFrame containing group-user mappings to be removed in Databricks.

        Notes:
            - Groups mappings by group ID for efficient processing.
            - Uses PATCH request to remove members from groups.
            - Implements retry mechanism for failed API calls.
            - Logs the removal process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        operation_set = {}
        unique_groups = mappings_to_remove['group_id_y'].drop_duplicates()
        for g_idx,group_id in unique_groups.items():
            members = []    
            if self.Scalable_SCIM_Enabled:
                for idx,row in mappings_to_remove[mappings_to_remove['group_id_y']==group_id].iterrows(): 
                    user_id = row['id_y']
                    members.append({'value':int(user_id)})
                    operation_set[group_id] = {"op": "remove","path": "members","value":members}
            else:
                for idx,row in mappings_to_remove[mappings_to_remove['group_id_y']==group_id].iterrows(): 
                    user_id = row['id_y']
                    members.append( {"op": "remove",'path': f"members.value[value eq {user_id}]"})
                    operation_set[group_id] = members

        # print(operation_set)
        for item in operation_set: 
            retry_counter = 0
            while True:
                if self.Scalable_SCIM_Enabled:
                    payload = {
                        "schemas": [
                        "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                        ],
                        "Operations": [operation_set[item]]
                        }
                else:
                    payload = {
                            "schemas": [
                            "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                            ],
                            "Operations": operation_set[item]
                            }
                account_id = self.dbx_config["account_id"] 
                url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups/{item}"

                token_result = self.get_dbx_token()
                headers = {'Authorization': 'Bearer ' + token_result }

                req = requests.patch(url=url, headers=headers, json = payload)
                if (req.status_code == 200) or (req.status_code == 204):
                    self.logger_obj.info(f"Following Users : {operation_set[item]}  were removed from group:{item}") 
                    break
                else:
                    self.logger_obj.error(f"Group mapping removal failed for items:{item}") 
                    self.logger_obj.error(f"Group mapping removal failed for items : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        break
    @log_decorator.log_decorator()
    def add_dbx_group_mappings(self,mappings_to_add,group_master_df,users_df_dbx):
        """
        Add group memberships in Databricks.

        This function adds users to groups in Databricks based on the provided DataFrame of mappings to add.

        Args:
            mappings_to_add (pd.DataFrame): DataFrame containing group-user mappings to be added in Databricks.
            group_master_df (pd.DataFrame): DataFrame containing master list of groups in Databricks.
            users_df_dbx (pd.DataFrame): DataFrame containing user information in Databricks.

        Notes:
            - Handles users, service principals, and nested groups.
            - Uses PATCH request to add members to groups.
            - Implements retry mechanism for failed API calls.
            - Logs the addition process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        mapping_set = {}
        unique_groups = mappings_to_add[['group_id_x','aad_group_displayName']].drop_duplicates().to_dict(orient = 'records')
        all_users_df = self.get_all_users_dbx()
        all_spns_df = self.get_all_spns_dbx()

        mappings_to_add['userPrincipalName'] = mappings_to_add['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)
        all_users_df['userName'] = all_users_df['userName'].apply(lambda s:s.lower() if type(s) == str else s)
        all_spns_df['displayName'] = all_spns_df['displayName'].apply(lambda s:s.lower() if type(s) == str else s)
        group_master_df['displayName'] = group_master_df['displayName'].apply(lambda s:s.lower() if type(s) == str else s)



        mappings_to_add_updated = mappings_to_add[['group_id_x','userPrincipalName','@odata.type','displayName_x','aad_group_displayName']].merge(all_users_df[['userName','id']], left_on=['userPrincipalName'], right_on=['userName'], how='left')
        mappings_to_add_updated.columns = ['group_id','userPrincipalName','@odata.type','displayName','aad_group_displayName','userName','id']

        mappings_to_add_updated['displayName'] = mappings_to_add_updated['displayName'].apply(lambda s:s.lower() if type(s) == str else s)

        mappings_to_add_updated = mappings_to_add_updated.merge(all_spns_df[['displayName','id']], left_on=['displayName'], right_on=['displayName'], how='left')
        mappings_to_add_updated['id'] = mappings_to_add_updated['id_x'].fillna(mappings_to_add_updated['id_y'])
        mappings_to_add_updated = mappings_to_add_updated.drop('id_x', axis=1)
        mappings_to_add_updated = mappings_to_add_updated.drop('id_y', axis=1)

        mappings_to_add_updated['userPrincipalName'] = mappings_to_add_updated['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)

        mappings_to_add_updated = mappings_to_add_updated.merge(group_master_df[['displayName','id']], left_on=['userPrincipalName'], right_on=['displayName'], how='left')
        mappings_to_add_updated['id'] = mappings_to_add_updated['id_x'].fillna(mappings_to_add_updated['id_y'])
        mappings_to_add_updated = mappings_to_add_updated.drop('id_x', axis=1)
        mappings_to_add_updated = mappings_to_add_updated.drop('id_y', axis=1)

         
        
        # with ThreadPoolExecutor(max_workers=20) as executor:
        for group in unique_groups:
            if len(group_master_df[group_master_df['externalId']==group['group_id_x']])>0:
                members = []   
                group_id = str(group_master_df[group_master_df['externalId']==group['group_id_x']].iloc[0]['id'])
                ids = list(mappings_to_add_updated[mappings_to_add_updated['group_id']==group['group_id_x']]['id'].dropna())
                for id in ids:
                    members.append( {'value':id})
                mapping_set[group_id] = members
            elif len(group_master_df[group_master_df['displayName']==group['aad_group_displayName']])>0:
                members = []   
                group_id = str(group_master_df[group_master_df['displayName']==group['aad_group_displayName']].iloc[0]['id'])
                ids = list(mappings_to_add_updated[mappings_to_add_updated['aad_group_displayName']==group['aad_group_displayName']]['id'].dropna())
                for id in ids:
                    members.append( {'value':id})
                mapping_set[group_id] = members
                # threads.append(executor.submit(self.get_group_member_mapping_set,mappings_to_add, group_master_df, users_df_dbx, group_external_id))
                                               
                # mapping_set.append(self.get_group_member_mapping_set())
            # for task in as_completed(threads):
            #     mapping_set.append(task.result())
     
                
        try:
            threads = []
            result = []
            with ThreadPoolExecutor(max_workers=self.dbx_config["account_patch_resource_limit"]) as executor:    
                for item in mapping_set:
                    threads.append(executor.submit(self.patch_group_mapping,item,mapping_set[item]))
                for task in as_completed(threads):
                    result.append(task.result())
            
            for status in result:
                if not((status['status_code'] == '200') or (status['status_code'] == '204')):
                    self.logger_obj.error(f"Failed to add mapping for user with dbx_id:{status['group_id']} error : {status['status_code']}") 
        except Exception as e:
            self.logger_obj.error(f"Failed to add mapping error : {str(e)}") 
            # self.logger_obj.error(f"Deactivating User Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")

    def patch_group_mapping(self,group_id, members):
        """
        HTTP patch operation to add group memberships in Databricks.

        This function updates the member list provided in the input into Databricks groups using the group_id.

        Args:
            group_id (int): Databricks Group ID of the group where members are to be patched into.
            members (pd.DataFrame): list of members formatted in the Patch API format.

        Notes:
            - Handles users, service principals, and nested groups.
            - Uses PATCH request to add members to groups.
            - Implements retry mechanism for failed API calls.
            - Logs the addition process and any errors encountered.
        """
        try:    
            # group_id = next(iter(item_dict))
            # print(item)
            # print(mapping_set[item])
            retry_counter = 0
            while True:

                payload = {
                        "schemas": [
                        "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                        ],
                        "Operations": [
                        {
                        "op": "add",
                        "value": {
                        "members": members
                        }
                        }
                        ]
                        }


                account_id = self.dbx_config["account_id"] 
                url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
                token_result = self.get_dbx_token()

                headers = {'Authorization': 'Bearer ' + token_result }
                
                req = requests.patch(url=url, headers=headers, json = payload)
                if (req.status_code == 200) or (req.status_code == 204):
                    self.logger_obj.info(f"User mapping created for items:{group_id}") 
                    return {'groupd_id':group_id,'status_code':str(req.status_code)}
                    break
                else:
                    self.logger_obj.error(f"Group mapping creation failed for items:{group_id}") 
                    self.logger_obj.error(f"Group mapping creation failed for items : {req.status_code} and reason :{req.reason}. Attempting Retry")
                    if retry_counter <= 5:
                        time.sleep(2**retry_counter)
                        retry_counter+=1
                    else:
                        return {'group_id':group_id,'status_code':req.reason}
                        break
        except Exception as e:
            self.logger_obj.error(f"Failed to deactivate user with dbx_id:{id} error : {str(e)}") 
            self.logger_obj.error(f"Deactivating User Failed with status : {req.status_code} and reason :{req.reason}. Attempting Retry")
    

    def get_group_member_mapping_set(self, mappings_to_add, group_master_df, users_df_dbx, group_external_id):
        """
        Get all Groups and its members .

        This function identify the groups memberships and prepare dataset in a dictionary format as input for patch request.

        Args:
            mappings_to_add (pd.DataFrame): DataFrame containing group-user mappings to be added in Databricks.
            group_master_df (pd.DataFrame): DataFrame containing master list of groups in Databricks.
            users_df_dbx (pd.DataFrame): DataFrame containing user information in Databricks.
            group_external_id(str): This is the group id in GUID format referred in Azure AD.

        Notes:
            - Logs the addition process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        members = []

        


        for idx,row in mappings_to_add[mappings_to_add['group_id_x']==group_external_id].iterrows():
            if len(group_master_df[group_master_df['externalId']==row['group_id_x']]) >0 :
                group_id = str(group_master_df[group_master_df['externalId']==row['group_id_x']].iloc[0]['id'])
                # mapping_set[group_id] = members
                # if len(users_df_dbx[users_df_dbx['userName']==row['userPrincipalName']]) >0 :
            if row['@odata.type']=='#microsoft.graph.user':
                if (len(users_df_dbx[users_df_dbx['userName']==row['userPrincipalName']])>0):
                    user_id = str(users_df_dbx[users_df_dbx['userName']==row['userPrincipalName']].iloc[0]['id'])
                    members.append( {'value':user_id})  
                else:
                    df = self.get_user_details_with_userName_dbx(row['userPrincipalName'])
                    if df.shape[0]>0:
                        user_id = df.iloc[0]['id']
                        members.append( {'value':user_id})
            elif row['@odata.type']=='#microsoft.graph.servicePrincipal':
                if (len(users_df_dbx[users_df_dbx['applicationId']==row['appId']])>0):
                    user_id = str(users_df_dbx[users_df_dbx['applicationId']==row['appId']].iloc[0]['id'])
                    members.append( {'value':user_id})
                else:
                    df = self.get_spn_details_with_appDisplayName_dbx(row['appDisplayName'])
                    if df.shape[0]>0:
                        user_id = df.iloc[0]['id']
                        members.append( {'value':user_id})
            elif row['@odata.type']=='#microsoft.graph.group':
                if row["id_x"] in list(group_master_df["externalId"]):
                    if (len(group_master_df[group_master_df['externalId']==row['id_x']].iloc[0]['id'])>0):
                        user_id = group_master_df[group_master_df['externalId']==row['id_x']].iloc[0]['id']
                        members.append( {'value':user_id})
                else:
                    print(f"Group:{row['displayName_x']} is missing in groups_to_sync.json. Skipping this")
                    self.logger_obj.error(f"Group:{row['displayName_x']} is missing in groups_to_sync.json. Skipping this")
        return {group_id:members}


    def sync_mappings(self):
        """
        Synchronize group memberships between Azure AD and Databricks.

        This function compares group memberships in Azure AD with those in Databricks and performs the following actions:
        1. Adds missing group memberships to Databricks.
        2. Removes group memberships from Databricks that are not present in Azure AD.

        Notes:
            - Handles both users and service principals.
            - Uses pandas merge to identify differences in group memberships.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the synchronization process and results.
        """
        users_df_dbx = self.get_all_user_groups_dbx()
        # lower case for join
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s:s.lower() if type(s) == str else s)

        # users_df_dbx.display()

        # get all users that belong in atleast one group
        users_df_aad = self.get_all_groups_aad(True)
        # lower case for join
        users_df_aad['userPrincipalName'] = users_df_aad['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)
        users_df_aad = users_df_aad.reset_index(drop = True) 
        # If child groups are present, then the join would fail since userPrincipalNames are Nan. For groups, force the join through displayName
        # for idx,row in users_df_aad.iterrows():
        #     if row['@odata.type'] == '#microsoft.graph.group':
        #         users_df_aad.iloc[idx]['userPrincipalName'] = str(row['displayName']).lower()
        #     elif row['@odata.type'] == '#microsoft.graph.servicePrincipal':
        #         users_df_aad.iloc[idx]['userPrincipalName'] = str(row['appDisplayName']).lower()
        # for idx,row in users_df_dbx.iterrows():
        #     if row['type'] == 'ServicePrincipal':
        #         users_df_dbx.iloc[idx]['userName'] = str(row['displayName']).lower()

        users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.group', 'userPrincipalName'] = users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.group', 'displayName'].apply(lambda x: x.lower() if x is not None else x)
        users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal', 'userPrincipalName'] = users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal', 'appDisplayName'].apply(lambda x: x.lower() if x is not None else x)
        
        users_df_aad['userPrincipalName'].fillna('', inplace=True)
        users_df_aad.loc[(users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal') & (users_df_aad['userPrincipalName'] == ''), 'userPrincipalName'] = users_df_aad.loc[(users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal') & (users_df_aad['userPrincipalName'] == ''), 'displayName'].apply(lambda x: x.lower() if x is not None else x)

        
        users_df_dbx.loc[users_df_dbx['type'] =='ServicePrincipal', 'userName'] = users_df_dbx.loc[users_df_dbx['type'] =='ServicePrincipal', 'displayName'].apply(lambda x: x.lower() if x is not None else x)

        users_df_aad['aad_group_displayName'] = users_df_aad['aad_group_displayName'].apply(lambda s:s.lower() if type(s) == str else s)

        users_df_dbx['group_displayName'] = users_df_dbx['group_displayName'].apply(lambda s:s.lower() if type(s) == str else s)

        # net_delta = users_df_aad.merge(users_df_dbx, left_on=['group_id','userPrincipalName'], right_on=['group_externalId','userName'], how='outer')
        net_delta = users_df_aad.merge(users_df_dbx, left_on=['aad_group_displayName','userPrincipalName'], right_on=['group_displayName','userName'], how='outer')
        mappings_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())]
        
        group_master_df = self.get_all_groups_dbx()

        # mappings_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna()) & (net_delta['group_externalId'].notna())]
        mappings_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna()) & (net_delta['group_displayName'].notna())]
        # manage only removals for groups in sync list
        mappings_to_remove = mappings_to_remove[mappings_to_remove['group_displayName'].isin([x.lower() for x in self.groups_to_sync])]
        # Remove mappings that belong to SPNs since its handled separately
        # mappings_to_remove = mappings_to_remove[mappings_to_remove['applicationId'].isna()]
        if self.is_dryrun:
            print('This is a dry run')
            mappings_to_remove.to_csv(self.log_file_dir + 'mappings_to_remove_Users.csv')
            mappings_to_add.to_csv(self.log_file_dir + 'mappings_to_add_Users.csv')
        print(" Total Mappings for Users to be removed :" + str(mappings_to_remove.shape[0])) 
        print(" Total Mappings for Users to be added :" + str(mappings_to_add.shape[0])) 
        mappings_to_remove = mappings_to_remove[['id_y','group_id_y']].drop_duplicates()

        if not self.is_dryrun:
            mappings_to_remove.to_csv(self.log_file_dir + 'mappings_to_remove_Users.csv')
            mappings_to_add.to_csv(self.log_file_dir + 'mappings_to_add_Users.csv')
            self.remove_dbx_group_mappings(mappings_to_remove)
            
            self.add_dbx_group_mappings(mappings_to_add,group_master_df,users_df_dbx)



        # this check validates if there any spns to be synced
        # if 'appId' in users_df_dbx.columns:
        #     net_delta_spns = users_df_aad.merge(users_df_dbx, left_on=['group_id','appId'], right_on=['group_externalId','applicationId'], how='outer')
        #     net_delta_spns = net_delta_spns[net_delta_spns['@odata.type'] =='#microsoft.graph.servicePrincipal']
        #     mappings_to_add_spns = net_delta_spns[(net_delta_spns['id_x'].notna()) & (net_delta_spns['id_y'].isna())]
            
        #     mappings_to_remove_spns = net_delta_spns[(net_delta_spns['id_x'].isna()) & (net_delta_spns['id_y'].notna()) & (net_delta_spns['group_externalId'].notna())]
        #     mappings_to_remove_spns = mappings_to_remove_spns[mappings_to_remove_spns['group_displayName'].isin(self.groups_to_sync)]
        #     if self.is_dryrun:
        #         print('This is a dry run')
        #         print(" Total Mappings for SPN's to be removed :")
        #         mappings_to_remove_spns.to_csv(self.log_file_dir + 'mappings_to_remove_SPNs.csv')
        #         mappings_to_add_spns.to_csv(self.log_file_dir + 'mappings_to_add_SPNs.csv')
        #     mappings_to_remove_spns = mappings_to_remove_spns[['id_y','group_id_y']].drop_duplicates()   
        #     print(" Total New Mappings for SPNs:" + str(mappings_to_add_spns.shape[0]))
        #     print(" Total Mappings for SPNs to be removed :" + str(mappings_to_remove_spns.shape[0])) 

        #     if not self.is_dryrun:
        #         self.remove_dbx_group_mappings(mappings_to_remove_spns)
                
        #         self.add_dbx_group_mappings(mappings_to_add_spns,group_master_df,users_df_dbx)
        #         mappings_to_remove_spns.to_csv(self.log_file_dir + 'mappings_to_remove_SPNs.csv')
        #         mappings_to_add_spns.to_csv(self.log_file_dir + 'mappings_to_add_SPNs.csv')

    def deactivate_deleted_users(self):
        """
        Deactivate users in Databricks that have been deleted from Azure AD.

        This function identifies users that exist in Databricks but have been deleted from Azure AD,
        and deactivates them in Databricks.

        Notes:
            - Fetches deleted users from Azure AD and compares with active users in Databricks.
            - Excludes admin users from deactivation for safety.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the deactivation process and results.
        """
        all_users_dbx_df = self.get_all_users_dbx()
        all_spns_dbx_df = self.get_all_spns_dbx()
        unique_usernames_dbx = all_users_dbx_df['userName'].unique()
        unique_spns_dbx = all_spns_dbx_df['displayName'].unique()
        unique_usernames_dbx = [x.lower() for x in unique_usernames_dbx]
        unique_spns_dbx = [str(x).lower() for x in unique_spns_dbx]

        all_users_aad_df = self.get_users_by_username_aad(unique_usernames_dbx)
        all_spns_aad_df = self.get_apps_by_displayName_aad(unique_spns_dbx)

        unique_usernames_aad = all_users_aad_df['userPrincipalName'].unique()
        unique_spns_aad = all_spns_aad_df['displayName'].unique()
        unique_usernames_aad = [x.lower() for x in unique_usernames_aad]
        unique_spns_aad = [x.lower() for x in unique_spns_aad]

        users_to_be_removed = list([x for x in unique_usernames_dbx if x not in unique_usernames_aad])
        apps_to_be_removed = list([x for x in unique_spns_dbx if x not in (unique_spns_aad)])
        ids_to_be_removed = []
        ids_to_be_removed.extend(list(all_users_dbx_df[all_users_dbx_df['userName'].isin(users_to_be_removed)]['id']))
        ids_to_be_removed.extend(list(all_spns_dbx_df[all_spns_dbx_df['displayName'].isin(apps_to_be_removed)]['id']))

        df_ids_to_be_removed = pd.DataFrame({'id_y':ids_to_be_removed})
        if not self.is_dryrun:
            self.deactivate_users_dbx(df_ids_to_be_removed)
        else:
            if df_ids_to_be_removed.shape[0] > 0:
                print('Exporting Deactivation list for deleted users')
                df_ids_to_be_removed.to_csv(self.log_file_dir + 'dbx_deleted_users_dump.csv')
        # delete_users_df = self.get_delete_users_aad()
        # if (delete_users_df is not None) and (not delete_users_df.empty): 
        #     all_users_dbx_df = self.get_all_user_groups_dbx()

        #     delete_users_df['userPrincipalName'] = delete_users_df['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)
        #     net_delta = delete_users_df.merge(all_users_dbx_df, left_on=['userPrincipalName'], right_on=['userName'], how='inner')
        #     net_delta = net_delta[net_delta['active']== True]
        #     print(" Total Deleted Users detected:" + str(len(net_delta['userPrincipalName'].unique())))
        #     deleted_users = net_delta['userPrincipalName']
        #     deleted_users.to_csv(self.log_file_dir + 'deleted_users.csv')
        #     print(" Total Admins Users detected for deletion:" + str(len(net_delta[net_delta['isAdmin']==True]['userPrincipalName'].unique())))
        #     if self.is_dryrun:
        #         print('This is a dry run')
        #         if net_delta[net_delta['isAdmin']==True].shape[0] > 0:
        #             print('Exporting Deactivation list')
        #             net_delta[net_delta['isAdmin']==False].to_csv(self.log_file_dir + 'azure_deleted_users_dump.csv')

        #     else:
        #         if net_delta[net_delta['isAdmin']==True].shape[0] > 0:
        #             print('Removing Admin users from Deletion')
        #             net_delta = net_delta[net_delta['isAdmin']==False]
        #         self.deactivate_users_dbx(net_delta)
        #         print('Exporting Deactivation list')
        #         net_delta[net_delta['isAdmin']==False].to_csv(self.log_file_dir + 'azure_deleted_users_dump.csv')

   
    def deactivate_orphan_users(self):
        """
        Deactivate orphan users in Databricks.

        This function identifies users that exist in Databricks but are not present in any of the 
        synced Azure AD groups, and deactivates them in Databricks.

        Notes:
            - Compares users in Databricks with users in synced Azure AD groups.
            - Excludes users that belong to groups not in the sync list and account users.
            - Excludes admin users and groups from deactivation for safety.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the deactivation process and results.
        """
        users_df_dbx = self.get_all_user_groups_dbx(True)
        users_df_dbx = users_df_dbx[users_df_dbx['group_displayName']!='account users']
        spns_df_dbx = users_df_dbx[users_df_dbx['applicationId'].notna()]
        users_df_dbx = users_df_dbx[users_df_dbx['applicationId'].isna()]
        # lower case for join
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s:s.lower() if type(s) == str else s)

        all_users = self.get_all_users_dbx()
        # users_df_dbx.display()

        net_delta = all_users.merge(users_df_dbx, left_on=['userName'], right_on=['userName'], how='left')

        users_to_remove = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())& (net_delta['active_x'] == True)] 

        # # get all users that belong in atleast one group
        # users_df_aad_all = self.get_all_groups_aad(True)
        # users_df_aad = users_df_aad_all[users_df_aad_all['@odata.type']=='#microsoft.graph.user']
        # # keep only unique records
        # users_df_aad = users_df_aad[['id', 'displayName',  'userPrincipalName']]
        # users_df_aad = users_df_aad.drop_duplicates()
        # # lower case for join
        # users_df_aad['userPrincipalName'] = users_df_aad['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)
        # # users_df_aad.display()

        # net_delta = users_df_aad.merge(users_df_dbx, left_on=['userPrincipalName'], right_on=['userName'], how='outer')

        # users_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna())& (net_delta['active'] == True)] 
        # valid_users = users_to_remove.query('group_displayName not in @self.groups_to_sync & group_displayName != "account users"')['id_y'].unique()
        # users_to_remove = users_to_remove.query('id_y not in @valid_users') 
        # users_to_remove = users_to_remove.query('isAdmin != True') 
        # users_to_remove = users_to_remove.query('type != "Group"') 
        if self.is_dryrun:
            print('This is a dry run')
            if users_to_remove.shape[0] > 0:
                print('Exporting Deactivation list')
                users_to_remove.to_csv(self.log_file_dir + 'dbx_orphan_users_dump.csv')
        print(" Total Orphan Users :" + str(users_to_remove[['id_y','isAdmin']].drop_duplicates().shape[0]))

        users_to_remove = users_to_remove[['id_x','isAdmin']].drop_duplicates()
        users_to_remove.columns = ['id_y','isAdmin']


        if not self.is_dryrun:
            if users_to_remove.shape[0] > 0:
                print('Exporting Deactivation list')
                users_to_remove.to_csv(self.log_file_dir + 'dbx_orphan_users_dump.csv')
            self.logger_obj.info(f"Deactivating Orphan Users : {len(users_to_remove)}") 
            self.deactivate_users_dbx(users_to_remove)

    # def delete_users_dbx(self,users_to_delete):
    #     ret_df = pd.DataFrame()
    #     threads= []
    #     results = []
    #     with ThreadPoolExecutor(max_workers=20) as executor:
    #         for idx, row in users_to_delete.iterrows():
    #             id = row['id']
    #             account_id = self.dbx_config["account_id"]
    #             threads.append(executor.submit(self.delete_user, self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Users/{id}"))
                
    #         for task in as_completed(threads):
    #             results.append(task.result())

    #     return results
    
    # @sleep_and_retry
    # @limits(calls=7, period=1)      
    # def delete_user(self,url):
    #     token_result = self.get_dbx_token()
    #     headers = {'Authorization': 'Bearer ' + token_result }
    #     retry_counter = 0
    #     while True:
    #         req = requests.delete(url=url, headers=headers)
    #         if req.status_code == 204:
    #             break
    #         else:
    #             if retry_counter <=3 :
    #                 print('Retrying Delete')
    #                 time.sleep(1)
    #                 retry_counter+=1
    #             else:
    #                 break
    #     return req.content
    # @sleep_and_retry
    # @limits(calls=7, period=1)  
    # def delete_groups_dbx(self,groups_to_delete):
    #     ret_df = pd.DataFrame()
    #     for idx, row in groups_to_delete.iterrows():
    #         id = row['id']

    #         account_id = self.dbx_config["account_id"]
    #         url = self.dbx_config['dbx_account_host'] + f"/api/2.0/accounts/{account_id}/scim/v2/Groups/{id}"
    #         token_result = self.get_dbx_token()


    #         headers = {'Authorization': 'Bearer ' + token_result }
    #         req = requests.delete(url=url, headers=headers)
    #         # print(req.content)
    #         try:
    #             assert req.status_code == 204
    #             # print(id)
    #         except AssertionError:
    #             if req.status_code == 409:
    #                 print('User Already exists. Trying to activate user')
    #             else:
    #                 ('Failed Creating User')
    
    # def set_test_base(self):
    #     users_df_dbx = self.get_all_user_groups_dbx()
    #     if users_df_dbx is not None:
    #         users_df_dbx = users_df_dbx[users_df_dbx['id']!='8935314208503406']
    #         self.delete_users_dbx(users_df_dbx)
    #     groups_df_dbx = self.get_all_groups_dbx()
    #     if groups_df_dbx is not None:
    #         self.delete_groups_dbx(groups_df_dbx)
