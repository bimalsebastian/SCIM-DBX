
import log_decorator
import asyncio
import aiohttp
from functools import lru_cache
import msal
import pandas as pd
import json
import numpy as np
from typing import Tuple, List, Dict, Any, Optional, Union
from pydantic import BaseModel
from io import BytesIO
import math
import time
from structlog import get_logger
import datetime
from aiohttp import BasicAuth, ClientResponseError, RequestInfo
from ratelimit import limits, RateLimitException, sleep_and_retry
from yarl import URL

class DBXConfig(BaseModel):
    account_id: str
    azure_tenant_id: str
    client_id: str
    client_secret: str
    dbx_account_host: str
    account_get_resource_limit: int
    account_post_resource_limit: int
    account_patch_resource_limit: int
    aws_user_id: Optional[str]
    aws_password: Optional[str]
    workspace_client_secret: Optional[str]

class AADConfig(BaseModel):
    client_id: str
    authority: str
    client_secret: str
    scope: List[str]

class SCIMIntegrator:
    """
    A class to integrate Azure Active Directory with Databricks Account Console using SCIM.
    
    This class provides functionality to sync users, groups, and their mappings between 
    Azure AD and Databricks. It supports both regular and scalable SCIM APIs, allowing 
    integration for up to 50,000 users.

    Attributes:
        config (AADConfig): Configuration for Azure AD connection.
        dbx_config (DBXConfig): Configuration for Databricks connection.
        groups_to_sync (List[str]): List of AD groups to be synced.
        token (str): Azure AD authentication token.
        token_dbx (str): Databricks authentication token.
        is_dryrun (bool): Flag to indicate if operations should be simulated.
        log_file_name (str): Name of the log file.
        log_file_dir (str): Directory for log files.
        logger (structlog.Logger): Logger object for the class.
        Scalable_SCIM_Enabled (bool): Flag to enable scalable SCIM APIs.
        cloud_provider (str): Cloud provider (Azure or AWS).
        session (aiohttp.ClientSession): Async HTTP session for API calls.
        rate_limiter_get (asyncio.Semaphore): Rate limiter for GET requests.
        rate_limiter_post (asyncio.Semaphore): Rate limiter for POST requests.
        rate_limiter_patch (asyncio.Semaphore): Rate limiter for PATCH requests.
    """
    
    def __init__(self, config: AADConfig, dbx_config: DBXConfig, groups_to_sync: List[str], 
                 log_file_name: str, log_file_dir: str, token_dbx: str = '', token: str = '', 
                 is_dryrun: bool = True, Scalable_SCIM_Enabled: bool = False, cloud_provider: str = 'Azure'):
        """
        Initialize the SCIMIntegrator instance.

        Args:
            config (AADConfig): Configuration for Azure AD connection.
            dbx_config (DBXConfig): Configuration for Databricks connection.
            groups_to_sync (List[str]): List of AD groups to be synced.
            log_file_name (str): Name of the log file.
            log_file_dir (str): Directory for log files.
            token_dbx (str, optional): Databricks authentication token. Defaults to ''.
            token (str, optional): Azure AD authentication token. Defaults to ''.
            is_dryrun (bool, optional): Flag to indicate if operations should be simulated. Defaults to True.
            Scalable_SCIM_Enabled (bool, optional): Flag to enable scalable SCIM APIs. Defaults to False.
            cloud_provider (str, optional): Cloud provider (Azure or AWS). Defaults to 'Azure'.
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
        self.logger = get_logger(log_file_name=self.log_file_name, log_dir=self.log_file_dir)
        self.Scalable_SCIM_Enabled = Scalable_SCIM_Enabled
        self.cloud_provider = cloud_provider
        self.session = None
        self.rate_limiter_get = asyncio.Semaphore(self.dbx_config.account_get_resource_limit)
        self.rate_limiter_post = asyncio.Semaphore(self.dbx_config.account_post_resource_limit)
        self.rate_limiter_patch = asyncio.Semaphore(self.dbx_config.account_patch_resource_limit)
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def make_graph_get_call(self, url: str, pagination: bool = True, params: Dict[str, Any] = None, key: str = '', headers_in: Dict[str, str] = None) -> Union[List[Dict[str, Any]], Tuple[str, List[Dict[str, Any]]]]:
        """
        Make an asynchronous GET request to the Microsoft Graph API.

        This function handles pagination and supports both regular and batch requests.
        It uses exponential backoff for retrying failed requests.

        Args:
            url (str): The API endpoint URL.
            pagination (bool, optional): Whether to handle pagination. Defaults to True.
            params (Dict[str, Any], optional): Query parameters for the request. Defaults to None.
            key (str, optional): Key for batch requests. Defaults to ''.
            headers_in (Dict[str, str], optional): Additional headers for the request. Defaults to None.

        Returns:
            Union[List[Dict[str, Any]], Tuple[str, List[Dict[str, Any]]]]: If key is provided, returns a tuple of (key, results).
                           Otherwise, returns a list of results.

        Raises:
            Logs errors for failed requests after retry attempts.
        """
        # if self.session is None:
        #     self.session = aiohttp.ClientSession()
        # async with self.rate_limiter_get: // rate limit not applicable for MSFT APIs
        token = await self.get_aad_token()
        headers = {'Authorization': f'Bearer {token}'}
        if headers_in:
            headers.update(headers_in)

        graph_results = []
        while url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params, timeout=30) as response:
                            response.raise_for_status()
                            graph_result = await response.json()

                if 'value' in graph_result:
                    graph_results.extend(graph_result['value'])
                else:
                    graph_results.append(graph_result)

                if pagination and '@odata.nextLink' in graph_result:
                    url = graph_result['@odata.nextLink']
                    params = None
                else:
                    url = None
            except Exception as e:
                self.logger.error(f"Error in make_graph_get_call: {str(e)}")
                break

        return (key, graph_results) if key else graph_results

    async def auth_aad(self, isAccount: bool = True):
        """
        Authenticate asynchronously with Azure Active Directory or Azure Databricks.

        This function obtains an authentication token either for Azure AD or Databricks,
        depending on the isAccount parameter. It uses client credentials flow for authentication.

        Args:
            isAccount (bool, optional): If True, authenticate with Azure Databricks. 
                                        If False, authenticate with Azure AD. Defaults to True.

        Raises:
            Logs errors for failed authentication attempts.
        """
        if isAccount:
            url = f"https://login.microsoftonline.com/{self.dbx_config.azure_tenant_id}/oauth2/v2.0/token"
            post_data = {
                'client_id': self.dbx_config.client_id,
                'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
                'client_secret': self.dbx_config.client_secret,
                'grant_type': 'client_credentials'
            }
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_post:
                    async with session.post(url, data=post_data, headers=headers) as response:
                        response.raise_for_status()
                        res = await response.json()
                        self.dbx_token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=res['expires_in'] - 30)
                        self.token_dbx = res['access_token']
        else:
            client = msal.ConfidentialClientApplication(
                self.config.client_id, 
                authority=self.config.authority, 
                client_credential=self.config.client_secret
            )
            token_result = client.acquire_token_for_client(scopes=self.config.scope)
            self.token = token_result['access_token']
            self.aad_token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=token_result['expires_in'] - 30)

    async def get_dbx_token(self):
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
        if datetime.datetime.now() >= self.dbx_token_expiry:
            if self.cloud_provider == 'Azure':
                await self.auth_aad(True)
            else:
                await self.auth_aws_dbx()
        return self.token_dbx

    async def get_aad_token(self):
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
        if datetime.datetime.now() >= self.aad_token_expiry:
            await self.auth_aad(False)
        return self.token

    async def auth_aws_dbx(self):
        """
        Authenticate asynchronously with Databricks on AWS and obtain an access token.

        This method performs authentication for Databricks workspaces hosted on AWS. It uses
        the account ID, user ID, and password stored in the dbx_config to obtain an access token.

        Notes:
            - Uses HTTP Basic Authentication with the AWS user ID and password.
            - Requests a token with 'all-apis' scope.
            - Updates self.token_dbx with the new access token.
            - Sets self.dbx_token_expiry based on the token's expiration time (with a 30-second buffer).
            - Raises an exception if the authentication request fails.

        Raises:
            aiohttp.ClientResponseError: If the authentication request fails.
        """
        url = f"https://accounts.cloud.databricks.com/oidc/accounts/{self.dbx_config.account_id}/v1/token"
        auth = BasicAuth(self.dbx_config.aws_user_id, self.dbx_config.aws_password)
        params = {'grant_type': 'client_credentials', 'scope': 'all-apis'}
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        async with aiohttp.ClientSession() as session:
            async with self.rate_limiter_post:
                async with session.post(url, auth=auth, data=params, headers=headers) as response:
                    response.raise_for_status()
                    res = await response.json()
                    self.dbx_token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=res['expires_in'] - 30)
                    self.token_dbx = res['access_token']

            

    async def get_spn_details(self, service_principals: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieve detailed information about service principals from Azure AD asynchronously.

        This function makes asynchronous Graph API calls to get detailed information for each service principal
        provided in the input DataFrame.

        Args:
            service_principals (pd.DataFrame): DataFrame containing service principal IDs.

        Returns:
            pd.DataFrame: DataFrame with detailed information about the service principals.

        Notes:
            - Uses asynchronous calls for improved performance.
            - Concatenates results for multiple service principals into a single DataFrame.
        """
        spns_df = pd.DataFrame()
        tasks = []
        for _, spn in service_principals.iterrows():
            url = f'https://graph.microsoft.com/v1.0/servicePrincipals/{spn["id"]}'
            tasks.append(self.make_graph_get_call(url, False))
        
        results = await asyncio.gather(*tasks)
        
        for res in results:
            res_json = json.dumps(res)
            df = pd.read_json(BytesIO(str.encode(res_json)), dtype='unicode', convert_dates=False)
            spns_df = pd.concat([spns_df, df], ignore_index=True)
        return spns_df

    # @lru_cache(maxsize=256)
    def get_all_groups_aad(self, with_members: bool = False) -> pd.DataFrame:
        """
        Retrieve all groups from Azure AD based on the groups_to_sync list asynchronously.

        This function fetches group information from Azure AD, optionally including member details.
        It uses batch processing and asynchronous calls for improved performance.

        Args:
            with_members (bool, optional): If True, include member details for each group. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame containing group information and optionally member details.

        Notes:
            - Uses caching to improve performance for repeated calls.
            - Implements asynchronous calls for parallel processing of API requests.
            - Handles pagination for large result sets.
            - Supports both user and service principal group members.
        """
        batch_size = 10
        split_count = math.ceil(len(self.groups_to_sync) / batch_size)
        groups_to_sync_split = np.array_split(self.groups_to_sync, max(split_count, 1))

        filter_params = []
        for group_set in groups_to_sync_split:
            quoted_values = []
            for value in group_set:
                quoted_values.append('"' + value + '"')
            values_string = ', '.join(quoted_values)
            filter_string = 'displayName in (' + values_string + ')'
            filter_params.append({'$filter': filter_string, '$select': 'id,displayName'})

        tasks = [
            self.make_graph_get_call('https://graph.microsoft.com/v1.0/groups', True, filter_param)
            for filter_param in filter_params
        ]
        async def _get_all_groups_aad():
            results = await asyncio.gather(*tasks)
            master_list = pd.concat([
                pd.DataFrame(result) for result in results
            ], ignore_index=True)

            if with_members:
                member_tasks = [
                    self.make_graph_get_call(
                        f'https://graph.microsoft.com/beta/groups/{row["id"]}/members',
                        True,
                        params={'$select': 'id, displayName, userPrincipalName, appDisplayName, appId'},
                        key=row['id']
                    )
                    for _, row in master_list.iterrows()
                ]

                member_results = await asyncio.gather(*member_tasks)
                user_groups_df = pd.concat([
                    pd.DataFrame(result[1]).assign(group_id=result[0], aad_group_displayName=master_list[master_list['id'] == result[0]]['displayName'].iloc[0].lower())
                    for result in member_results
                ], ignore_index=True)

                return user_groups_df
            else:
                return master_list
        return _get_all_groups_aad()

    async def get_all_groups_nested_aad(self, parent_groups: List[str]) -> Optional[pd.DataFrame]:
        """
        Retrieve nested group information from Azure Active Directory for given parent groups asynchronously.

        This function fetches detailed information about the specified parent groups and their nested subgroups
        from Azure AD. It uses batch processing and asynchronous calls for improved performance.

        Args:
            parent_groups (List[str]): List of parent group names to fetch nested group information for.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing nested group information, or None if no groups are found.

        Notes:
            - Implements batching to handle large numbers of parent groups efficiently.
            - Uses asynchronous calls to make concurrent API requests, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Fetches both group details and their member groups.
            - Concatenates results into a single DataFrame with group hierarchy information.
        """
        batch_size = 10
        split_count = math.ceil(len(parent_groups) / batch_size)
        groups_to_sync_split = np.array_split(parent_groups, max(split_count, 1))

        filter_params = []
        for group_set in groups_to_sync_split:
            quoted_values = []
            for value in group_set:
                quoted_values.append('"' + value + '"')
            values_string = ', '.join(quoted_values)
            filter_string = 'displayName in (' + values_string + ')'
            filter_params.append({'$filter': filter_string, '$select': 'id,displayName'})

        tasks = [
            self.make_graph_get_call('https://graph.microsoft.com/v1.0/groups', True, filter_param)
            for filter_param in filter_params
        ]

        results = await asyncio.gather(*tasks)
        master_list = pd.concat([
            pd.DataFrame(result) for result in results
        ], ignore_index=True)

        member_tasks = [
            self.make_graph_get_call(
                f'https://graph.microsoft.com/beta/groups/{row["id"]}/members/microsoft.graph.group',
                True,
                params={'$select': 'id, displayName'},
                key=row['id']
            )
            for _, row in master_list.iterrows()
        ]

        member_results = await asyncio.gather(*member_tasks)
        user_groups_df = pd.concat([
            pd.DataFrame(result[1]).assign(
                group_id=result[0],
                aad_group_displayName=master_list[master_list['id'] == result[0]]['displayName'].iloc[0].lower()
            )
            for result in member_results
        ], ignore_index=True)

        return user_groups_df if not user_groups_df.empty else None

    def dump_json(self, new_groups_to_sync: List[str], path: str):
        """
        Write a list of groups to a JSON file.

        This function takes a list of group names and writes them to a JSON file at the specified path.

        Args:
            new_groups_to_sync (List[str]): List of group names to be written to the JSON file.
            path (str): File path where the JSON file will be created or overwritten.

        Notes:
            - Uses json.dump with indentation for better readability of the output file.
            - Ensures proper encoding of non-ASCII characters.
        """
        with open(path, 'w') as outfile:
            json.dump(new_groups_to_sync, outfile, ensure_ascii=False, indent=4)

    async def populate_groups_to_sync(self, path: str):
        """
        Populate and update the groups_to_sync list with nested groups asynchronously.

        This function recursively fetches nested groups starting from the initial groups_to_sync list,
        and updates the list with all discovered nested groups. The final list is then saved to a JSON file.

        Args:
            path (str): File path where the updated groups_to_sync list will be saved as a JSON file.

        Notes:
            - Iteratively fetches nested groups until no new groups are discovered.
            - Updates the groups_to_sync list with newly discovered groups in each iteration.
            - Saves the final, comprehensive list of groups to the specified JSON file.
            - Uses asynchronous calls for improved performance.
        """
        iter_groups_to_sync = self.groups_to_sync.copy()
        while True:
            all_groups = await self.get_all_groups_nested_aad(iter_groups_to_sync)
            if all_groups is None:
                break
            new_parents = list(all_groups[~all_groups['displayName'].isin(iter_groups_to_sync)]['displayName'])
            if not new_parents:
                break
            iter_groups_to_sync.extend(new_parents)
        
        self.dump_json(iter_groups_to_sync, path)

    async def get_all_groups_aad_member_count(self) -> pd.DataFrame:
        """
        Retrieve all groups from Azure AD along with their member counts asynchronously.

        This function fetches information about all groups in Azure AD and calculates
        the number of members for each group using asynchronous calls.

        Returns:
            pd.DataFrame: DataFrame containing group information including id, displayName, and member_count.

        Notes:
            - Uses the Graph API to fetch group information and member counts.
            - Implements asynchronous calls to handle large numbers of groups efficiently.
            - Calculates the total member count across all groups.
        """
        url = 'https://graph.microsoft.com/v1.0/groups'
        params = {'$select': 'id, displayName'}
        resp = await self.make_graph_get_call(url, True, params=params)
        groups_df = pd.DataFrame(resp)
        groups_df['member_count'] = 0

        async def get_member_count(group_id):
            url = f'https://graph.microsoft.com/v1.0/groups/{group_id}/members/'
            params = {'$count': 'true', 'ConsistencyLevel': 'eventual', '$select': 'id'}
            resp = await self.make_graph_get_call(url, True, params=params)
            return group_id, resp.get('@odata.count', len(resp))

        tasks = [get_member_count(row['id']) for _, row in groups_df.iterrows()]
        results = await asyncio.gather(*tasks)

        for group_id, count in results:
            groups_df.loc[groups_df['id'] == group_id, 'member_count'] = count

        return groups_df

    # @lru_cache(maxsize=256)
    async def get_all_users_aad(self) -> pd.DataFrame:
        """
        Retrieve all users from Azure AD asynchronously.

        This function fetches user information from Azure AD using the Microsoft Graph API.

        Returns:
            pd.DataFrame: DataFrame containing user information.

        Notes:
            - Uses caching to improve performance for repeated calls.
            - Handles pagination for large result sets.
            - Implements asynchronous API calls for improved performance.
        """
        url = 'https://graph.microsoft.com/v1.0/users'
        users = await self.make_graph_get_call(url, pagination=True)
        users_json = json.dumps(users)
        users_df = pd.read_json(BytesIO(str.encode(users_json)), dtype='unicode', convert_dates=False)
        return users_df

    async def get_users_by_username_aad(self, usernames: List[str]) -> pd.DataFrame:
        """
        Retrieve user information from Azure AD for a list of usernames asynchronously.

        This function fetches user details from Azure AD based on the provided list of usernames (userPrincipalNames).
        It uses batch processing and asynchronous calls for improved performance when dealing with a large number of users.

        Args:
            usernames (List[str]): List of userPrincipalNames to fetch user information for.

        Returns:
            pd.DataFrame: DataFrame containing user information (id and userPrincipalName) for the specified usernames.

        Notes:
            - Implements batching to handle large numbers of usernames efficiently.
            - Uses asynchronous calls to make concurrent API requests, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Concatenates results from multiple API calls into a single DataFrame.
        """
        batch_size = 10
        usernames_split = np.array_split(usernames, max(math.ceil(len(usernames) / batch_size), 1))

        tasks = []
        for user_batch in usernames_split:
            filter_expression = ' or '.join([f"userPrincipalName eq '{value}'" for value in user_batch])
            filter_param = {'$filter': filter_expression, '$select': 'id,userPrincipalName'}
            url = 'https://graph.microsoft.com/v1.0/users'
            tasks.append(self.make_graph_get_call(url, True, filter_param))

        results = await asyncio.gather(*tasks)
        master_list = pd.concat([pd.DataFrame(result) for result in results], ignore_index=True)
        return master_list

    async def get_apps_by_displayName_aad(self, displayNames: List[str]) -> pd.DataFrame:
        """
        Retrieve application information from Azure AD for a list of display names asynchronously.

        This function fetches application details from Azure AD based on the provided list of display names.
        It uses batch processing and asynchronous calls for improved performance when dealing with a large number of applications.

        Args:
            displayNames (List[str]): List of application display names to fetch information for.

        Returns:
            pd.DataFrame: DataFrame containing application information (appId and displayName) for the specified display names.

        Notes:
            - Implements batching to handle large numbers of display names efficiently.
            - Uses asynchronous calls to make concurrent API requests, improving performance.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Concatenates results from multiple API calls into a single DataFrame.
        """
        batch_size = 10
        displayName_split = np.array_split(displayNames, max(math.ceil(len(displayNames) / batch_size), 1))

        tasks = []
        for app_batch in displayName_split:
            filter_expression = ' or '.join([f"displayName eq '{value}'" for value in app_batch])
            filter_param = {'$filter': filter_expression, '$select': 'appId,displayName'}
            url = 'https://graph.microsoft.com/v1.0/applications'
            tasks.append(self.make_graph_get_call(url, True, filter_param))

        results = await asyncio.gather(*tasks)
        master_list = pd.concat([pd.DataFrame(result) for result in results], ignore_index=True)
        return master_list

    async def get_delete_users_aad(self) -> pd.DataFrame:
        """
        Retrieve deleted users from Azure AD asynchronously.

        This function fetches information about recently deleted users from Azure AD 
        using the Microsoft Graph API.

        Returns:
            pd.DataFrame: DataFrame containing information about deleted users.

        Notes:
            - Handles pagination for large result sets.
            - Cleans up the userPrincipalName for deleted users by removing appended IDs.
            - Implements asynchronous API calls for improved performance.
        """
        url = "https://graph.microsoft.com/v1.0/directory/deletedItems/microsoft.graph.user"
        params = {
            '$count': 'true',
            '$orderby': 'deletedDateTime asc',
            '$select': 'id,DisplayName,userPrincipalName,deletedDateTime'
        }
        headers = {'ConsistencyLevel': 'eventual'}
        deleted_users = await self.make_graph_get_call(url, pagination=True, params=params, headers_in=headers)
        deleted_users_df = pd.DataFrame(deleted_users)

        if not deleted_users_df.empty:
            deleted_users_df['userPrincipalName'] = deleted_users_df.apply(
                lambda row: row['userPrincipalName'].replace(row['id'].replace('-', ''), ''),
                axis=1
            )
        return deleted_users_df

    async def get_user_details_dbx(self, ids_string: str) -> pd.DataFrame:
        """
        Retrieve user details from Databricks using SCIM API asynchronously.

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
            - Implements asynchronous API calls with retry logic for improved reliability.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users"
            params = {'filter': ids_string}
            
            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            for retry in range(6):
                try:
                    req_json = await make_request()
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        raise
                    await asyncio.sleep(1)

            df = pd.DataFrame({
                'displayName': pd.Series(dtype='str'),
                'active': pd.Series(dtype='bool'),
                'id': pd.Series(dtype='str'),
                'userName': pd.Series(dtype='str'),
                'applicationId': pd.Series(dtype='str'),
                'externalId': pd.Series(dtype='str'),
                'isAdmin': pd.Series(dtype='bool'),
            }, index=range(len(req_json['Resources'])))

            for counter, resource in enumerate(req_json['Resources']):
                df.loc[counter, 'displayName'] = resource.get('displayName', '')
                df.loc[counter, 'isAdmin'] = any(role['value'] == 'account_admin' for role in resource.get('roles', []))
                df.loc[counter, 'active'] = resource.get('active', False)
                df.loc[counter, 'id'] = resource['id']
                df.loc[counter, 'userName'] = resource['userName']
                df.loc[counter, 'applicationId'] = np.nan
                df.loc[counter, 'externalId'] = resource.get('externalId', '')

            return df
        
        except Exception as e:
            self.logger.error(f"Fetching User Details Failed with error: {str(e)}")
            raise

    async def get_spn_details_dbx(self, ids_string: str) -> pd.DataFrame:
        """
        Retrieve service principal details from Databricks using SCIM API asynchronously.

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
            - Implements asynchronous API calls with retry logic for improved reliability.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            params = {'filter': ids_string}
            
            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            for retry in range(6):
                try:
                    req_json = await make_request()
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        raise
                    await asyncio.sleep(1)

            df = pd.DataFrame({
                'displayName': pd.Series(dtype='str'),
                'active': pd.Series(dtype='bool'),
                'id': pd.Series(dtype='str'),
                'userName': pd.Series(dtype='str'),
                'applicationId': pd.Series(dtype='str'),
                'externalId': pd.Series(dtype='str'),
            }, index=range(len(req_json['Resources'])))

            for counter, resource in enumerate(req_json['Resources']):
                df.loc[counter, 'displayName'] = resource.get('displayName', '')
                df.loc[counter, 'active'] = resource.get('active', False)
                df.loc[counter, 'id'] = resource['id']
                df.loc[counter, 'userName'] = np.nan
                df.loc[counter, 'applicationId'] = resource['applicationId']
                df.loc[counter, 'externalId'] = resource.get('externalId', '')

            return df
        except Exception as e:
            self.logger.error(f"Fetching Service Principal Details Failed with error: {str(e)}")
            raise

    async def get_all_user_groups_dbx(self, all_groups: bool = False) -> pd.DataFrame:
        """
        Retrieve all user groups and their members from Databricks using SCIM API asynchronously.

        This function fetches information about all groups and their members (users, service principals, and nested groups)
        from Databricks. It supports the scalable SCIM API and handles pagination for large datasets.

        Args:
            all_groups (bool): If True, fetch all groups. If False, only fetch groups in self.groups_to_sync.

        Returns:
            pd.DataFrame: DataFrame containing group and member information.

        Notes:
            - Implements batching and pagination to handle large numbers of groups.
            - Processes users, service principals, and nested groups separately.
            - Uses asynchronous calls for parallel processing of API calls.
            - Implements retry mechanism for failed API calls.
            - Supports both regular and scalable SCIM APIs.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups"
            
            async def fetch_groups(params):
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            graph_results = []
            if not all_groups:
                filter_batch_size = math.ceil(len(self.groups_to_sync) / 30)
                group_ids_filter = np.array_split(self.groups_to_sync, max(filter_batch_size, 1))
                
                for group_filter in group_ids_filter:
                    if self.Scalable_SCIM_Enabled:
                        filter_string = '" or displayName eq "'.join(group_filter)
                        filter_string = 'displayName eq "' + filter_string + '"'
                    else:
                        filter_string = '` or displayName eq `'.join(group_filter)
                        filter_string = 'displayName eq `' + filter_string + '`'

                    start_index = 1
                    while True:
                        params = {'startIndex': start_index, 'count': 100, 'filter': filter_string}
                        for retry in range(6):
                            try:
                                result = await fetch_groups(params)
                                graph_results.append(result)
                                start_index += result['itemsPerPage']
                                if start_index > result['totalResults']:
                                    break
                                break
                            except aiohttp.ClientResponseError as e:
                                if e.status != 429 or retry == 5:
                                    raise
                                await asyncio.sleep(1)
                        if start_index > result['totalResults']:
                            break
            else:
                start_index = 1
                while True:
                    params = {'startIndex': start_index, 'count': 100}
                    for retry in range(6):
                        try:
                            result = await fetch_groups(params)
                            graph_results.append(result)
                            start_index += result['itemsPerPage']
                            if start_index > result['totalResults']:
                                break
                            break
                        except aiohttp.ClientResponseError as e:
                            if e.status != 429 or retry == 5:
                                raise
                            await asyncio.sleep(2**retry)
                    if start_index > result['totalResults']:
                        break

            if not self.Scalable_SCIM_Enabled:
                groups_df, user_ids, spn_ids, group_ids = await self.extract_group_members([result['Resources'] for result in graph_results])
            else:
                groups_df, user_ids, spn_ids, group_ids = await self.extract_group_members_scalable(graph_results)

            group_list_df = pd.DataFrame({
                'displayName': pd.Series(dtype='str'),
                'active': pd.Series(dtype='bool'),
                'id': pd.Series(dtype='str'),
                'userName': pd.Series(dtype='str'),
                'applicationId': pd.Series(dtype='str')
            }, index=range(len(group_ids)))

            for counter, group_id in enumerate(group_ids):
                group_data = groups_df[groups_df['group_id'] == group_id]
                if not group_data.empty:
                    group_list_df.loc[counter] = [
                        group_data.iloc[0]['group_displayName'],
                        True,
                        group_id,
                        group_data.iloc[0]['group_displayName'],
                        np.nan
                    ]
                else:
                    self.logger.warning(f"Group with ID: {group_id} is not present in groups_to_sync.sh file")

            group_list_df['type'] = 'Group'
            user_list_df = await self.get_users_with_ids_dbx(user_ids)
            user_list_df['type'] = 'User'
            spn_list_df = await self.get_spns_with_ids_dbx(spn_ids)
            spn_list_df['type'] = 'ServicePrincipal'

            user_list_df = pd.concat([user_list_df, spn_list_df, group_list_df], ignore_index=True)
            user_list_df = user_list_df.merge(groups_df, left_on=['id'], right_on=['user_id'], how='inner')
            user_list_df = user_list_df[user_list_df['id'].notna()]

            return user_list_df

        except Exception as e:
            self.logger.error(f"Error in get_all_user_groups_dbx: {str(e)}")
            raise

    async def extract_group_members(self, resource_items: List[Dict]) -> Tuple[pd.DataFrame, List[str], List[str], List[str]]:
        """
        Extract member information from group resources asynchronously.

        This function processes a list of group resources and extracts member information,
        categorizing them into users, service principals, and nested groups.

        Args:
            resource_items (List[Dict]): List of group resource items containing member information.

        Returns:
            Tuple[pd.DataFrame, List[str], List[str], List[str]]: A tuple containing:
                - pd.DataFrame: DataFrame with group and member information.
                - List[str]: List of user IDs.
                - List[str]: List of service principal IDs.
                - List[str]: List of nested group IDs.

        Notes:
            - Handles both groups with members and groups without members.
            - Categorizes members into users, service principals, and nested groups based on their reference URLs.
            - Efficiently processes large numbers of groups and members using pandas operations.
        """
        groups_data = []
        ids = []

        for resource in resource_items:
            if 'members' in resource:
                for group in resource['members']:
                    groups_data.append({
                        'group_displayName': resource['displayName'],
                        'group_externalId': resource.get('externalId'),
                        'group_id': resource['id'],
                        'user_id': group['value']
                    })
                    ids.append(group['$ref'])
            else:
                groups_data.append({
                    'group_displayName': resource['displayName'],
                    'group_externalId': resource.get('externalId'),
                    'group_id': resource['id'],
                    'user_id': np.nan
                })

        groups_df = pd.DataFrame(groups_data)

        user_ids = [id.replace('Users/', '') for id in ids if 'Users' in id]
        spn_ids = [id.replace('ServicePrincipals/', '') for id in ids if 'ServicePrincipals' in id]
        group_ids = [id.replace('Groups/', '') for id in ids if 'Groups' in id]

        return groups_df, user_ids, spn_ids, group_ids

    async def extract_group_members_scalable(self, graph_results: List[Dict]) -> Tuple[pd.DataFrame, List[str], List[str], List[str]]:
        """
        Extract member information from group results in a scalable manner for Scalable SCIM APIs.

        This function processes graph results containing group information, retrieves detailed
        group information for each group, and extracts member details. It's designed to handle
        large numbers of groups efficiently using asynchronous calls.

        Args:
            graph_results (List[Dict]): List of graph result items containing group information.

        Returns:
            Tuple[pd.DataFrame, List[str], List[str], List[str]]: A tuple containing:
                - pd.DataFrame: DataFrame with group and member information.
                - List[str]: List of user IDs.
                - List[str]: List of service principal IDs.
                - List[str]: List of nested group IDs.

        Notes:
            - Uses asynchronous calls to fetch group details concurrently.
            - Respects the account_get_resource_limit from dbx_config for concurrency control.
            - Calls extract_group_members to process the detailed group information.
            - Suitable for processing large numbers of groups efficiently.
        """
        group_ids = [
            resource['id']
            for result in graph_results
            if 'Resources' in result
            for resource in result['Resources']
        ]

        async def get_group_details(group_id): 
            return await self.get_group_details_with_id(group_id)

        tasks = [get_group_details(group_id) for group_id in group_ids]
        group_details = await asyncio.gather(*tasks)

        # Flatten the list of results
        group_details = [item for sublist in group_details for item in sublist]

        return await self.extract_group_members(group_details)

    async def get_group_details_with_id(self, group_id: str) -> List[Dict]:
        """
        Retrieve detailed information for a specific group from Databricks asynchronously.

        This function fetches detailed information for a given group ID from Databricks
        using the SCIM API. It implements retry logic for rate limiting.

        Args:
            group_id (str): The ID of the group to fetch details for.

        Returns:
            List[Dict]: A list containing the JSON response with group details.

        Raises:
            Exception: If there's an error in fetching group details, it logs the error and re-raises.

        Notes:
            - Uses the Databricks SCIM API to fetch group details.
            - Implements retry logic with a 1-second delay for rate limit (429) responses.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"

            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers) as response:
                            for retry in range(6):
                                try:
                                    response.raise_for_status()
                                    return await response.json()
                                except aiohttp.ClientResponseError as e:
                                    if e.status != 429 or retry == 5:
                                        print(f'Retry failed for group : {group_id} : Retry : {retry}')
                                        raise
                                    else:
                                        print(f'Retrying for group : {group_id} : Retry : {retry} : Sleep for : {2**retry}')
                                    await asyncio.sleep(2**retry)
            
            result = await make_request()
            return [result]

        except Exception as e:
            self.logger.error(f"Getting Group Details with Id Failed: {str(e)}")
            raise

    async def get_users_with_ids_dbx(self, ids: List[str]) -> pd.DataFrame:
        """
        Retrieve user details from Databricks for a list of user IDs asynchronously.

        This function fetches user information from Databricks for the given list of user IDs.
        It supports batch processing to handle large numbers of users efficiently.

        Args:
            ids (List[str]): List of user IDs to fetch from Databricks.

        Returns:
            pd.DataFrame: DataFrame containing user details for the given IDs.

        Notes:
            - Implements batching to handle large numbers of user IDs.
            - Uses asynchronous calls for the get_user_details_dbx method.
            - Handles potential errors for each batch separately.
        """
        try:
            batch_size = 100
            ids = np.unique(ids)
            id_batches = np.array_split(ids, max(1, len(ids) // batch_size))

            async def fetch_batch(id_set):
                ids_string = ' or '.join(f'id eq {id}' for id in id_set)
                return await self.get_user_details_dbx(ids_string)

            tasks = [fetch_batch(id_set) for id_set in id_batches]
            results = await asyncio.gather(*tasks)

            return pd.concat(results, ignore_index=True)

        except Exception as e:
            self.logger.error(f"Getting User Details with Ids Failed: {str(e)}")
            raise

    async def get_user_details_with_userName_dbx(self, nameString: str) -> pd.DataFrame:
        """
        Retrieve user details from Databricks for a specific username asynchronously.

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
            - Implements asynchronous API calls with retry logic for improved reliability.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users"
            params = {'filter': f'userName eq "{nameString}"'}
            
            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            for retry in range(6):
                try:
                    req_json = await make_request()
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        raise
                    await asyncio.sleep(2 ** retry)  # Exponential backoff

            df = pd.DataFrame({
                'displayName': pd.Series(dtype='str'),
                'active': pd.Series(dtype='bool'),
                'id': pd.Series(dtype='str'),
                'userName': pd.Series(dtype='str'),
                'applicationId': pd.Series(dtype='str'),
                'externalId': pd.Series(dtype='str'),
                'isAdmin': pd.Series(dtype='bool'),
            }, index=range(len(req_json['Resources'])))

            for counter, resource in enumerate(req_json['Resources']):
                df.loc[counter, 'displayName'] = resource.get('displayName', '')
                df.loc[counter, 'isAdmin'] = any(role['value'] == 'account_admin' for role in resource.get('roles', []))
                df.loc[counter, 'active'] = resource.get('active', False)
                df.loc[counter, 'id'] = resource['id']
                df.loc[counter, 'userName'] = resource['userName']
                df.loc[counter, 'applicationId'] = np.nan
                df.loc[counter, 'externalId'] = resource.get('externalId', '')

            return df
        
        except Exception as e:
            self.logger.error(f"Fetching User Details Failed with error: {str(e)}")
            raise

    async def get_spn_details_with_appDisplayName_dbx(self, appDisplayName: str) -> pd.DataFrame:
        """
        Retrieve service principal details from Databricks for a specific application display name asynchronously.

        This function fetches service principal information from Databricks for the given application display name.

        Args:
            appDisplayName (str): Application display name to search for in Databricks.

        Returns:
            pd.DataFrame: DataFrame containing service principal details for the given application display name.

        Raises:
            Exception: If the API call fails, logs the error and re-raises the exception.

        Notes:
            - Similar to get_spn_details_dbx, but filters by application display name instead of ID.
            - Implements asynchronous API calls with retry logic for improved reliability.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            params = {'filter': f'displayName eq `{appDisplayName}`'}
            
            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            for retry in range(6):
                try:
                    req_json = await make_request()
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        raise
                    await asyncio.sleep(2 ** retry)  # Exponential backoff

            if 'Resources' in req_json:
                df = pd.DataFrame({
                    'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')
                }, index=range(len(req_json['Resources'])))

                for counter, resource in enumerate(req_json['Resources']):
                    df.loc[counter, 'displayName'] = resource.get('displayName', '')
                    df.loc[counter, 'active'] = resource.get('active', False)
                    df.loc[counter, 'id'] = resource['id']
                    df.loc[counter, 'userName'] = np.nan
                    df.loc[counter, 'applicationId'] = resource['applicationId']
                    df.loc[counter, 'externalId'] = resource.get('externalId', '')
            else:
                df = pd.DataFrame({
                    'displayName': pd.Series(dtype='str'),
                    'active': pd.Series(dtype='bool'),
                    'id': pd.Series(dtype='str'),
                    'userName': pd.Series(dtype='str'),
                    'applicationId': pd.Series(dtype='str'),
                    'externalId': pd.Series(dtype='str')
                })

            return df
        except Exception as e:
            self.logger.error(f"Fetching Service Principal Details Failed with error: {str(e)}")
            raise

    async def get_spns_with_ids_dbx(self, ids: List[str]) -> pd.DataFrame:
        """
        Retrieve service principal details from Databricks for a list of service principal IDs asynchronously.

        This function fetches service principal information from Databricks for the given list of IDs.
        It supports batch processing to handle large numbers of service principals efficiently.

        Args:
            ids (List[str]): List of service principal IDs to fetch from Databricks.

        Returns:
            pd.DataFrame: DataFrame containing service principal details for the given IDs.

        Notes:
            - Implements batching to handle large numbers of service principal IDs.
            - Uses asynchronous calls for the get_spn_details_dbx method.
            - Handles potential errors for each batch separately.
        """
        try:
            batch_size = 100
            ids = np.unique(ids)
            id_batches = np.array_split(ids, max(1, len(ids) // batch_size))

            async def fetch_batch(id_set):
                ids_string = ' or '.join(f'id eq {id}' for id in id_set)
                return await self.get_spn_details_dbx(ids_string)

            tasks = [fetch_batch(id_set) for id_set in id_batches]
            results = await asyncio.gather(*tasks)

            return pd.concat(results, ignore_index=True)

        except Exception as e:
            self.logger.error(f"Getting SPN Details with Ids Failed: {str(e)}")
            raise

    async def get_all_groups_dbx(self) -> pd.DataFrame:
        """
        Retrieve all groups from Databricks using SCIM API asynchronously.

        This function fetches information about all groups from Databricks. 
        It supports pagination to handle large numbers of groups.

        Returns:
            pd.DataFrame: DataFrame containing group information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of groups.
            - Uses asynchronous API calls with retry logic for improved reliability.
            - Includes group entitlements if available.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups"
            
            async def fetch_groups(start_index, count):
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                params = {'startIndex': start_index, 'count': count}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            graph_results = []
            start_index = 1
            count = 100

            while True:
                for retry in range(6):
                    try:
                        result = await fetch_groups(start_index, count)
                        graph_results.append(result)
                        start_index += count
                        if start_index > result['totalResults']:
                            break
                        
                        break
                    except aiohttp.ClientResponseError as e:
                        if e.status != 429 or retry == 5:
                            raise
                        await asyncio.sleep(2 ** retry)  # Exponential backoff
                if start_index > result['totalResults']:
                    break

            df = pd.DataFrame(columns=["displayName", "externalId", "id", "entitlements"])
            for result in graph_results:
                for resource in result['Resources']:
                    _df = pd.DataFrame([{
                        "displayName": resource.get("displayName"),
                        "externalId": resource.get("externalId"),
                        "id": resource["id"],
                        "entitlements": resource.get("entitlements")
                    }])
                    df = pd.concat([_df,df])

            return df
        except Exception as e:
            self.logger.error(f"Getting All Group Details Failed: {str(e)}")
            raise
    async def get_all_admins_dbx(self) -> pd.DataFrame:
        """
        Retrieve all admin users from Databricks using SCIM API asynchronously.

        This function fetches information about all users with admin roles from Databricks.
        It supports pagination to handle large numbers of admin users.

        Returns:
            pd.DataFrame: DataFrame containing admin user information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of admin users.
            - Uses asynchronous API calls with retry logic for improved reliability.
            - Filters users based on the 'account_admin' role.
        """
        account_id = self.dbx_config.account_id
        url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users"
        
        async def fetch_admins(start_index, count):
            token = await self.get_dbx_token()
            headers = {'Authorization': f'Bearer {token}'}
            params = {'startIndex': start_index, 'count': count, 'filter': 'roles.value co account_admin'}
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_get:
                    async with session.get(url, headers=headers, params=params) as response:
                        response.raise_for_status()
                        return await response.json()

        graph_results = []
        start_index = 1
        count = 100

        while True:
            for retry in range(6):
                try:
                    result = await fetch_admins(start_index, count)
                    graph_results.append(result)
                    if start_index + count > result['totalResults']:
                        break
                    start_index += count
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        raise
                    await asyncio.sleep(2 ** retry)  # Exponential backoff
            if start_index > result['totalResults']:
                break

        df = pd.DataFrame(columns=["displayName", "userName", "active", "id"])
        for result in graph_results:
            for resource in result['Resources']:
                if any(role['value'] == 'account_admin' for role in resource.get('roles', [])):
                    df = df.append({
                        "displayName": resource["displayName"],
                        "userName": resource["userName"],
                        "active": resource["active"],
                        "id": resource["id"]
                    }, ignore_index=True)

        return df

    async def get_workspaces_list(self) -> pd.DataFrame:
        """
        Retrieve a list of Databricks workspaces and their permissions asynchronously.

        This function fetches information about all workspaces in the Databricks account
        and attempts to retrieve user permissions for each workspace.

        Returns:
            pd.DataFrame: DataFrame containing workspace information and permissions.

        Notes:
            - Requires workspace-level authentication for each workspace.
            - Fetches user entitlements for each workspace.
            - May encounter permission issues for workspaces where the service principal lacks access.
            - Uses asynchronous calls for improved performance.
        """
        account_id = self.dbx_config.account_id
        url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/workspaces"
        token = await self.get_dbx_token()
        headers = {'Authorization': f'Bearer {token}'}
        async with aiohttp.ClientSession() as session:
            async with self.rate_limiter_get:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    workspaces = await response.json()

        df = pd.DataFrame(workspaces)
        df['permissions'] = ''

        async def fetch_workspace_permissions(workspace):
            token_url = f"https://{workspace['deployment_name']}.azuredatabricks.net/oidc/v1/token"
            token_data = {
                'grant_type': 'client_credentials',
                'scope': 'all-apis'
            }
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_post:
                    async with self.rate_limiter_post:
                        async with session.post(token_url, auth=aiohttp.BasicAuth(self.dbx_config.client_id, self.dbx_config.workspace_client_secret), data=token_data) as response:
                            response.raise_for_status()
                            token_result = await response.json()    
            
            perm_url = f"https://{workspace['deployment_name']}.azuredatabricks.net/api/2.0/preview/scim/v2/Users"
            perm_headers = {'Authorization': f'Bearer {token_result["access_token"]}'}
            perm_params = {'attributes': 'entitlements'}
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_get:
                    async with session.get(perm_url, headers=perm_headers, params=perm_params) as response:
                        if response.status == 200:
                            perm_data = await response.json()
                            return workspace['deployment_name'], perm_data['Resources']
                        else:
                            return workspace['deployment_name'], None

        tasks = [fetch_workspace_permissions(workspace) for _, workspace in df.iterrows()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for deployment_name, permissions in results:
            if permissions:
                df.loc[df['deployment_name'] == deployment_name, 'permissions'] = str(permissions)

        return df

    async def get_all_users_dbx(self) -> pd.DataFrame:
        """
        Retrieve all users from Databricks using SCIM API asynchronously.

        This function fetches information about all users from Databricks.
        It supports pagination to handle large numbers of users.

        Returns:
            pd.DataFrame: DataFrame containing user information from Databricks.

        Notes:
            - Implements pagination to handle large numbers of users.
            - Uses asynchronous API calls with retry logic for improved reliability.
            - Includes active status for each user.
        """
        try:
            results = await self.get_all_users_call_api(1, 100)
            total_results = results['totalResults']
            
            tasks = []
            for start_index in range(1, total_results, 100):
                tasks.append(self.get_all_users_call_api(start_index, 100))
            
            all_results = await asyncio.gather(*tasks)

            df = pd.DataFrame(columns=["displayName", "userName", "active", "id"])
            for result in all_results:
                for resource in result['Resources']:
                    _df = pd.DataFrame([{
                        "displayName": resource["displayName"],
                        "userName": resource["userName"],
                        "active": resource["active"],
                        "id": resource["id"]
                    }])
                    df = pd.concat([_df,df])

            return df.drop_duplicates()
        except Exception as e:
            self.logger.error(f"Getting All User Details Failed: {str(e)}")
            raise

    async def get_all_users_call_api(self, index: int, itemsPerPage: int) -> Dict:
        """
        Make an asynchronous API call to retrieve user information from Databricks.

        This function fetches a page of user information from Databricks using the SCIM API.
        It supports pagination and implements retry logic for improved reliability.

        Args:
            index (int): The starting index for the user list to retrieve.
            itemsPerPage (int): The number of items to retrieve per page.

        Returns:
            Dict: JSON response containing user information for the requested page.

        Notes:
            - Uses the Databricks SCIM API to fetch user details.
            - Implements retry logic with exponential backoff for rate-limited requests.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
            - Logs errors and retry attempts for debugging purposes.
        """
        account_id = self.dbx_config.account_id
        url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users"
        
        async def make_request():
            token = await self.get_dbx_token()
            headers = {'Authorization': f'Bearer {token}'}
            params = {'startIndex': index, 'count': itemsPerPage}
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_get:
                    async with session.get(url, headers=headers, params=params) as response:
                        response.raise_for_status()
                        return await response.json()

        for retry in range(6):
            try:
                return await make_request()
            except aiohttp.ClientResponseError as e:
                if e.status != 429 or retry == 5:
                    self.logger.error(f"Fetching All User Details Failed with status : {e.status} and reason: {e.message}. Retry Failed. Continuing")
                    raise
                wait_time = 2 ** retry
                self.logger.error(f"Fetching All User Details Failed with status : {e.status} and reason: {e.message}. Attempting Retry in {wait_time} seconds")
                await asyncio.sleep(wait_time)

        return {}  # This line should never be reached due to the raise in the loop, but it's here for completeness

    async def get_all_spns_dbx(self) -> pd.DataFrame:
        """
        Retrieve all service principals from Databricks asynchronously.

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
            - Uses retry logic with exponential backoff for rate-limited requests.
            - Utilizes get_dbx_token to ensure a valid authentication token is used.
            - Processes the JSON response and converts it into a pandas DataFrame for easy manipulation.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
            
            async def fetch_spns(start_index, count):
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                params = {'startIndex': start_index, 'count': count}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json()

            first_page = await fetch_spns(1, 100)
            total_results = first_page['totalResults']
            
            tasks = [fetch_spns(start_index, 100) for start_index in range(1, total_results, 100)]
            all_results = await asyncio.gather(*tasks)

            df = pd.DataFrame(columns=["id", "applicationId", "displayName", "externalId", "active"])
            for result in all_results:
                for resource in result['Resources']:
                    _df = pd.DataFrame([{
                        "id": resource["id"],
                        "applicationId": resource["applicationId"],
                        "displayName": resource.get("displayName", ""),
                        "externalId": resource.get("externalId", ""),
                        "active": resource["active"]
                    }])
                    df = pd.concat([_df, df])

            return df
        except Exception as e:
            self.logger.error(f"Getting All Service Principal Details Failed: {str(e)}")
            raise

    @log_decorator.log_decorator()
    async def create_group_dbx(self, displayName: str, externalId: str) -> pd.DataFrame:
        """
        Create a new group in Databricks using SCIM API asynchronously.

        This function creates a new group in Databricks with the given display name and external ID.

        Args:
            displayName (str): The display name for the new group.
            externalId (str): The external ID for the new group (typically the Azure AD group ID).

        Returns:
            pd.DataFrame: DataFrame containing the details of the newly created group.

        Notes:
            - Uses retry mechanism with exponential backoff for failed API calls.
            - Logs the creation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
        """
        try:
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups"

            payload = {
                "displayName": displayName,
                "externalId": externalId
            }

            async def make_request():
                token = await self.get_dbx_token()
                headers = {'Authorization': f'Bearer {token}'}
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_post:
                        async with session.post(url, headers=headers, json=payload) as response:
                            response.raise_for_status()
                            return await response.json()

            for retry in range(6):
                try:
                    result = await make_request()
                    return pd.DataFrame([result])
                except aiohttp.ClientResponseError as e:
                    if (e.status != 429 and e.status !=409) or  retry == 5:
                        self.logger.error(f"Creating User Group Failed with status: {e.status} and reason: {e.message}. Retry Failed. Continuing")
                        raise
                    if e.status ==409:
                        self.logger.error(f"Creating User Group Failed with status: {e.status} and reason: {e.message}. Skipping Retry. Continuing")
                        break

                    wait_time = 2 ** retry
                    self.logger.error(f"Creating User Group Failed with status: {e.status} and reason: {e.message}. Attempting Retry in {wait_time} seconds")
                    await asyncio.sleep(wait_time)

        except Exception as e:
            self.logger.error(f"Creating User Group Failed: {str(e)}")
            raise

    @sleep_and_retry
    @limits(calls=7, period=1)
    async def create_users_request(self, userName: str, displayName: str, externalId: str) -> int:
        """
        Create a new user in Databricks using SCIM API asynchronously.

        This function creates a new user in Databricks with the given username, display name, and external ID.

        Args:
            userName (str): The username for the new user.
            displayName (str): The display name for the new user.
            externalId (str): The external ID for the new user (typically the Azure AD user ID).

        Returns:
            int: The HTTP status code of the create operation.

        Notes:
            - Implements rate limiting (7 calls per second).
            - Uses retry mechanism with exponential backoff for failed API calls.
            - Logs the creation process and any errors encountered.
        """
        account_id = self.dbx_config.account_id
        url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users"
        payload = {
            "userName": userName,
            "displayName": displayName,
            "externalId": externalId
        }
        token = await self.get_dbx_token()
        headers = {'Authorization': f'Bearer {token}'}
        async def make_request():
            
            async with aiohttp.ClientSession() as session:
                async with self.rate_limiter_post:
                    async with session.post(url, headers=headers, json=payload) as response:
                        return response.status, await response.text()

        for retry in range(6):
            try:
                status, text = await make_request()
                if status in [201, 409]:  # 201: Created, 409: Conflict (User already exists)
                    return status
                request_info = RequestInfo(URL(url), "POST", headers)
                # raise aiohttp.ClientResponseError(request_info=None, history=None, status=status, message=f"Unexpected status code: {status}")
                raise ClientResponseError(request_info=request_info, history=(), status=status, 
                                      message=f"Unexpected status code: {status}\n{text}")
                   
            except aiohttp.ClientResponseError as e:
                if e.status != 429 or retry == 5:
                    self.logger.error(f"Failed Creating User {userName}: {e.message}")
                    raise
                wait_time = 2 ** retry
                self.logger.error(f"Failed Creating User {userName}: {e.message}. Attempting Retry in {wait_time} seconds")
                await asyncio.sleep(wait_time)

    @sleep_and_retry
    @limits(calls=7, period=1)
    async def create_spns_request(self, applicationId: str, displayName: str, externalId: str) -> int:
        """
        Create a new service principal in Databricks using SCIM API asynchronously.

        This function creates a new service principal in Databricks with the given application ID, display name, and external ID.

        Args:
            applicationId (str): The application ID for the new service principal.
            displayName (str): The display name for the new service principal.
            externalId (str): The external ID for the new service principal (typically the Azure AD service principal ID).

        Returns:
            int: The HTTP status code of the create operation.

        Notes:
            - Implements rate limiting (7 calls per second).
            - Uses retry mechanism with exponential backoff for failed API calls.
            - Logs the creation process and any errors encountered.
            - Handles different payload structures for Azure and AWS cloud providers.
        """
        account_id = self.dbx_config.account_id
        url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"

        if self.cloud_provider == 'AWS':
            payload = {
                "displayName": displayName,
                "externalId": externalId
            }
            filter_value = f'displayName eq "{displayName}"'
            params = {'filter': filter_value}
        else:  # Azure
            payload = {
                "applicationId": applicationId,
                "displayName": displayName,
                "externalId": externalId
            }
            params = None

        token = await self.get_dbx_token()
        headers = {'Authorization': f'Bearer {token}'}
        async def make_request(check_existing=False):
            if check_existing:
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_get:
                        async with session.get(url, headers=headers, params=params) as response:
                            response.raise_for_status()
                            return await response.json(), await response.text()
            else:
                async with aiohttp.ClientSession() as session:
                    async with self.rate_limiter_post:
                        async with session.post(url, headers=headers, json=payload) as response:
                            return response.status

        for retry in range(6):
            try:
                if self.cloud_provider == 'AWS':
                    # Check if SPN already exists
                    existing, text =  await make_request(check_existing=True)
                    if existing.get('Resources'):
                        self.logger.error(f"Service Principal with display name {displayName} already exists")
                        return 200  # Assuming 200 for existing resource
                
                status = await make_request()
                if status == 201:  # Created
                    return status
                
                request_info = RequestInfo(URL(url), "POST", headers)
                raise ClientResponseError(request_info=request_info, history=(), status=status, 
                                      message=f"Unexpected status code: {status}\n{text}")
            except aiohttp.ClientResponseError as e:
                if e.status != 429 or retry == 5:
                    self.logger.error(f"Failed Creating Service Principal {displayName} with application id {applicationId}: {e.message}")
                    raise
                wait_time = 2 ** retry
                self.logger.error(f"Failed Creating Service Principal {displayName} with application id {applicationId}: {e.message}. Attempting Retry in {wait_time} seconds")
                await asyncio.sleep(wait_time)

    @log_decorator.log_decorator()
    async def create_users_dbx(self, users_to_add: pd.DataFrame) -> List[int]:
        """
        Create multiple users in Databricks using SCIM API asynchronously.

        This function creates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_add (pd.DataFrame): DataFrame containing user information to be added to Databricks.

        Returns:
            List[int]: A list of HTTP status codes for each user creation operation.

        Notes:
            - Uses asyncio.gather for concurrent processing of user creations.
            - Calls create_users_request for each individual user creation.
            - Decorated with log_decorator for additional logging.
        """
        # tasks = [
        #     self.create_users_request(
        #         row['userPrincipalName'],
        #         row['displayName_x'],
        #         row['id_x']
        #     )
        #     for _, row in users_to_add.iterrows()
        # ]
        tasks = []
        for _, row in users_to_add.iterrows(): 
            tasks.append(
                self.create_users_request(
                    row['userPrincipalName'],
                    row['displayName_x'],
                    row['id_x']
                )
                
            )

        return await asyncio.gather(*tasks)

    @log_decorator.log_decorator()
    async def create_spns_dbx(self, spns_to_add: pd.DataFrame) -> List[int]:
        """
        Create multiple service principals in Databricks using SCIM API asynchronously.

        This function creates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_add (pd.DataFrame): DataFrame containing service principal information to be added to Databricks.

        Returns:
            List[int]: A list of HTTP status codes for each service principal creation operation.

        Notes:
            - Uses asyncio.gather for concurrent processing of service principal creations.
            - Calls create_spns_request for each individual service principal creation.
            - Decorated with log_decorator for additional logging.
        """
        tasks = [
            self.create_spns_request(
                row['appId'],
                row['displayName_x'],
                row['id_x']
            )
            for _, row in spns_to_add.iterrows()
        ]
        return await asyncio.gather(*tasks)
    @log_decorator.log_decorator()    
    async def deactivate_users_dbx(self, users_to_remove: pd.DataFrame):
        """
        Deactivate multiple users in Databricks using SCIM API asynchronously.

        This function deactivates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_remove (pd.DataFrame): DataFrame containing user information to be deactivated in Databricks.

        Notes:
            - Uses PATCH request to update user status to inactive.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the deactivation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Uses asyncio.gather for concurrent processing of user deactivations.
        """
        account_id = self.dbx_config.account_id
        
        async def deactivate_user(user_id: str):
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users/{user_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "replace", "value": {"active": False}}]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status == 200:
                                    self.logger.info(f"Deactivated User: {user_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Failed to deactivate user with dbx_id: {user_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [deactivate_user(row['id_y']) for _, row in users_to_remove.iterrows()]
        await asyncio.gather(*tasks)

    @log_decorator.log_decorator()    
    async def deactivate_spns_dbx(self, spns_to_remove: pd.DataFrame):
        """
        Deactivate multiple service principals in Databricks using SCIM API asynchronously.

        This function deactivates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_remove (pd.DataFrame): DataFrame containing service principal information to be deactivated in Databricks.

        Notes:
            - Uses PATCH request to update service principal status to inactive.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the deactivation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Uses asyncio.gather for concurrent processing of service principal deactivations.
        """
        account_id = self.dbx_config.account_id
        
        async def deactivate_spn(spn_id: str):
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{spn_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "replace", "value": {"active": False}}]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status == 200:
                                    self.logger.info(f"Deactivated SPN: {spn_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Failed to deactivate SPN with dbx_id: {spn_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [deactivate_spn(row['id_y']) for _, row in spns_to_remove.iterrows()]
        await asyncio.gather(*tasks)

    @log_decorator.log_decorator()
    async def activate_users_dbx(self, users_to_activate: pd.DataFrame):
        """
        Activate multiple users in Databricks using SCIM API asynchronously.

        This function activates multiple users in Databricks based on the provided DataFrame of user information.

        Args:
            users_to_activate (pd.DataFrame): DataFrame containing user information to be activated in Databricks.

        Notes:
            - Uses PATCH request to update user status to active.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the activation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Uses asyncio.gather for concurrent processing of user activations.
        """
        account_id = self.dbx_config.account_id
        
        async def activate_user(user_id: str):
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users/{user_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "replace", "value": {"active": True}}]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status == 200:
                                    self.logger.info(f"Activated User: {user_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Failed to activate user with dbx_id: {user_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [activate_user(row['id_y']) for _, row in users_to_activate.iterrows()]
        await asyncio.gather(*tasks)

    @log_decorator.log_decorator()
    async def activate_spns_dbx(self, spns_to_activate: pd.DataFrame):
        """
        Activate multiple service principals in Databricks using SCIM API asynchronously.

        This function activates multiple service principals in Databricks based on the provided DataFrame of service principal information.

        Args:
            spns_to_activate (pd.DataFrame): DataFrame containing service principal information to be activated in Databricks.

        Notes:
            - Uses PATCH request to update service principal status to active.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the activation process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Uses asyncio.gather for concurrent processing of service principal activations.
        """
        account_id = self.dbx_config.account_id
        
        async def activate_spn(spn_id: str):
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{spn_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "replace", "value": {"active": True}}]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status == 200:
                                    self.logger.info(f"Activated SPN: {spn_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Failed to activate SPN with dbx_id: {spn_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [activate_spn(row['id_y']) for _, row in spns_to_activate.iterrows()]
        await asyncio.gather(*tasks)

    async def sync_groups(self):
        """
        Synchronize groups between Azure AD and Databricks asynchronously.

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
            - Implements asynchronous operations for improved performance.
        """
        groups_df_dbx = await self.get_all_groups_dbx()
        groups_df_aad = await self.get_all_groups_aad(False)

        if self.is_dryrun:
            print('This is a dry run')

        if 'externalId' in groups_df_dbx.columns:
            net_delta = groups_df_aad.merge(groups_df_dbx, left_on=['displayName','id'], right_on=['displayName','externalId'], how='outer')
            groups_to_add = net_delta[net_delta['id_y'].isna()]
            groups_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['externalId'].notna())] 
            print(f" Total New Groups: {groups_to_add.shape[0]}")
            print(f" Total Groups that could be deleted: {groups_to_remove.shape[0]} (Info Only: Deactivation will not be done)")
        else:
            net_delta = groups_df_aad.merge(groups_df_dbx, left_on=['displayName'], right_on=['displayName'], how='outer')
            groups_to_add = net_delta[net_delta['id_y'].isna()]
            print(f" Total New Groups: {groups_to_add.shape[0]}")

        if not self.is_dryrun:
            # add missing groups into dbx
            created_df = pd.DataFrame()
            tasks = [self.create_group_dbx(row['displayName'], row['id_x']) for _, row in groups_to_add.iterrows()]
            results = await asyncio.gather(*tasks)
            created_df = pd.concat([created_df] + results, ignore_index=True)
            
            # Whats the grounds for removing a group
            # remove unmanaged groups from dbx
            ret_res = []
            # for idx, row in groups_to_remove.iterrows():
            #     id = row['id_y']
            #     ret_res.append(delete_group_dbx(id))

            return created_df, ret_res

    async def sync_users(self):
        """
        Synchronize users and service principals between Azure AD and Databricks asynchronously.

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
            - Implements asynchronous operations for improved performance.
        """
        users_df_dbx = await self.get_all_user_groups_dbx()
        spns_df_dbx = users_df_dbx[users_df_dbx['applicationId'].notna()]
        users_df_dbx = users_df_dbx[users_df_dbx['applicationId'].isna()]
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s:s.lower() if type(s) == str else s)

        users_df_aad_all = await self.get_all_groups_aad(True)
        users_df_aad = users_df_aad_all[users_df_aad_all['@odata.type']=='#microsoft.graph.user']
        users_df_aad = users_df_aad[['id', 'displayName', 'userPrincipalName']].drop_duplicates()
        users_df_aad['userPrincipalName'] = users_df_aad['userPrincipalName'].apply(lambda s:s.lower() if type(s) == str else s)

        net_delta = users_df_aad.merge(users_df_dbx, left_on=['userPrincipalName'], right_on=['userName'], how='outer')

        users_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())]
        users_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna()) & (net_delta['active'] == True)] 
        users_to_activate = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].notna()) & (net_delta['active'] == False)]

        if self.is_dryrun:
            print('This is a dry run')
        print(f" Total New Users: {users_to_add.shape[0]}")
        print(f" Total Users that could be deactivated: {users_to_remove.shape[0]} (Info Only: Deactivation will not be done)")
        print(f" Total Users that need to be activated: {users_to_activate.shape[0]}")

        if not self.is_dryrun:
            self.logger.info(f"Creating New Users: {len(users_to_add)}") 
            created_users = await self.create_users_dbx(users_to_add)
            self.logger.info(f"Deactivating Users: {len(users_to_remove)} (Info Only: Deactivation will not be done)") 
            # await self.deactivate_users_dbx(users_to_remove)
            self.logger.info(f"Activating Users: {len(users_to_activate)}") 
            await self.activate_users_dbx(users_to_activate)

        spn_df_aad = users_df_aad_all[users_df_aad_all['@odata.type']=='#microsoft.graph.servicePrincipal']
        spn_df_aad = spn_df_aad[['id', 'displayName','appId']].drop_duplicates()

        net_delta = spn_df_aad.merge(spns_df_dbx, left_on=['displayName'], right_on=['displayName'], how='outer')

        spns_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())]
        spns_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['applicationId'].notna()) & (net_delta['active'] == True)] 
        spns_to_activate = net_delta[(net_delta['id_x'].notna()) & (net_delta['applicationId'].notna()) & (net_delta['active'] == False)]

        if self.is_dryrun:
            print('This is a dry run')
        print(f" Total New SPNs: {spns_to_add.shape[0]}")
        print(f" Total SPNs that could be deactivated: {spns_to_remove.shape[0]} (Info Only: Deactivation will not be done)")
        print(f" Total SPNs that need to be activated: {spns_to_activate.shape[0]}")

        if not self.is_dryrun:
            created_spns = await self.create_spns_dbx(spns_to_add)
            await self.activate_spns_dbx(spns_to_activate)

        if not self.is_dryrun:
            created_users.extend(created_spns)
            return created_users
        
    @log_decorator.log_decorator()
    async def remove_dbx_group_mappings(self, mappings_to_remove: pd.DataFrame):
        """
        Remove group memberships in Databricks asynchronously.

        This function removes users from groups in Databricks based on the provided DataFrame of mappings to remove.

        Args:
            mappings_to_remove (pd.DataFrame): DataFrame containing group-user mappings to be removed in Databricks.

        Notes:
            - Groups mappings by group ID for efficient processing.
            - Uses PATCH request to remove members from groups.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the removal process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Implements asynchronous operations for improved performance.
        """
        operation_set = {}
        unique_groups = mappings_to_remove['group_id_y'].drop_duplicates()
        for g_idx, group_id in unique_groups.items():
            members = []    
            if self.Scalable_SCIM_Enabled:
                for _, row in mappings_to_remove[mappings_to_remove['group_id_y']==group_id].iterrows(): 
                    user_id = row['id_y']
                    members.append({'value': int(user_id)})
                    operation_set[group_id] = {"op": "remove", "path": "members", "value": members}
            else:
                for _, row in mappings_to_remove[mappings_to_remove['group_id_y']==group_id].iterrows(): 
                    user_id = row['id_y']
                    members.append({"op": "remove", 'path': f"members.value[value eq {user_id}]"})
                    operation_set[group_id] = members

        async def remove_mapping(group_id, operations):
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": operations if isinstance(operations, list) else [operations]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status in [200, 204]:
                                    self.logger.info(f"Removed mappings for group: {group_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Group mapping removal failed for group: {group_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [remove_mapping(group_id, operations) for group_id, operations in operation_set.items()]
        await asyncio.gather(*tasks)

    @log_decorator.log_decorator()
    async def add_dbx_group_mappings(self, mappings_to_add: pd.DataFrame, group_master_df: pd.DataFrame, users_df_dbx: pd.DataFrame):
        """
        Add group memberships in Databricks asynchronously.

        This function adds users to groups in Databricks based on the provided DataFrame of mappings to add.

        Args:
            mappings_to_add (pd.DataFrame): DataFrame containing group-user mappings to be added in Databricks.
            group_master_df (pd.DataFrame): DataFrame containing master list of groups in Databricks.
            users_df_dbx (pd.DataFrame): DataFrame containing user information in Databricks.

        Notes:
            - Handles users, service principals, and nested groups.
            - Uses PATCH request to add members to groups.
            - Implements retry mechanism with exponential backoff for failed API calls.
            - Logs the addition process and any errors encountered.
            - Decorated with log_decorator for additional logging.
            - Implements asynchronous operations for improved performance.
        """
        mapping_set = {}
        unique_groups = mappings_to_add[['group_id_x', 'aad_group_displayName']].drop_duplicates().to_dict(orient='records')
        all_users_df = await self.get_all_users_dbx()
        all_spns_df = await self.get_all_spns_dbx()

        mappings_to_add['userPrincipalName'] = mappings_to_add['userPrincipalName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        all_users_df['userName'] = all_users_df['userName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        all_spns_df['displayName'] = all_spns_df['displayName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        group_master_df['displayName'] = group_master_df['displayName'].apply(lambda s: s.lower() if isinstance(s, str) else s)

        mappings_to_add_updated = mappings_to_add[['group_id_x','userPrincipalName','@odata.type','displayName_x','aad_group_displayName']].merge(all_users_df[['userName', 'id']], left_on='userPrincipalName', right_on='userName', how='left')
        mappings_to_add_updated.columns = ['group_id', 'userPrincipalName', '@odata.type', 'displayName', 'aad_group_displayName', 'userName', 'id']

        mappings_to_add_updated['displayName'] = mappings_to_add_updated['displayName'].apply(lambda s: s.lower() if isinstance(s, str) else s)

        mappings_to_add_updated = mappings_to_add_updated.merge(all_spns_df[['displayName', 'id']], left_on='displayName', right_on='displayName', how='left')
        mappings_to_add_updated['id'] = mappings_to_add_updated['id_x'].fillna(mappings_to_add_updated['id_y'])
        mappings_to_add_updated = mappings_to_add_updated.drop(['id_x', 'id_y'], axis=1)

        mappings_to_add_updated['userPrincipalName'] = mappings_to_add_updated['userPrincipalName'].apply(lambda s: s.lower() if isinstance(s, str) else s)

        mappings_to_add_updated = mappings_to_add_updated.merge(group_master_df[['displayName', 'id']], left_on='userPrincipalName', right_on='displayName', how='left')
        mappings_to_add_updated['id'] = mappings_to_add_updated['id_x'].fillna(mappings_to_add_updated['id_y'])
        mappings_to_add_updated = mappings_to_add_updated.drop(['id_x', 'id_y'], axis=1)

        for group in unique_groups:
            members = []
            group_data = mappings_to_add_updated[mappings_to_add_updated['group_id'] == group['group_id_x']]
            
            if len(group_master_df[group_master_df['externalId'] == group['group_id_x']]) > 0:
                group_id = str(group_master_df[group_master_df['externalId'] == group['group_id_x']].iloc[0]['id'])                
                ids = list(mappings_to_add_updated[mappings_to_add_updated['group_id']==group['group_id_x']]['id'].dropna())
                for id in ids:
                    members.append( {'value':id})
                mapping_set[group_id] = members
            elif len(group_master_df[group_master_df['displayName'] == group['aad_group_displayName']]) > 0:
                group_id = str(group_master_df[group_master_df['displayName'] == group['aad_group_displayName']].iloc[0]['id'])
                ids = list(mappings_to_add_updated[mappings_to_add_updated['aad_group_displayName']==group['aad_group_displayName']]['id'].dropna())
                for id in ids:
                    members.append( {'value':id})
                mapping_set[group_id] = members
            else:
                self.logger.warning(f"Group not found: {group['aad_group_displayName']}")
                continue


        async def add_mapping(group_id: str, members: List[Dict[str, str]]):
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "add", "value": {"members": members}}]
            }

            for retry in range(6):
                try:
                    token = await self.get_dbx_token()
                    headers = {'Authorization': f'Bearer {token}'}
                    async with aiohttp.ClientSession() as session:
                        async with self.rate_limiter_patch:
                            async with session.patch(url, headers=headers, json=payload) as response:
                                if response.status == 204 or response.status == 200:
                                    self.logger.info(f"User mapping created for group: {group_id}")
                                    return
                                elif response.status != 429:
                                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Group mapping creation failed for group: {group_id}. Error: {str(e)}")
                        return
                await asyncio.sleep(2 ** retry)

        tasks = [add_mapping(group_id, members) for group_id, members in mapping_set.items()]
        await asyncio.gather(*tasks)

    async def sync_mappings(self):
        """
        Synchronize group memberships between Azure AD and Databricks asynchronously.

        This function compares group memberships in Azure AD with those in Databricks and performs the following actions:
        1. Adds missing group memberships to Databricks.
        2. Removes group memberships from Databricks that are not present in Azure AD.

        Notes:
            - Handles both users and service principals.
            - Uses pandas merge to identify differences in group memberships.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the synchronization process and results.
            - Implements asynchronous operations for improved performance.
        """
        users_df_dbx = await self.get_all_user_groups_dbx()
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        users_df_aad = await self.get_all_groups_aad(True)

        users_df_aad['userPrincipalName'] = users_df_aad['userPrincipalName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        users_df_aad = users_df_aad.reset_index(drop=True)

        
        users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.group', 'userPrincipalName'] = users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.group', 'displayName'].apply(lambda x: x.lower())
        users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal', 'userPrincipalName'] = users_df_aad.loc[users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal', 'appDisplayName'].apply(lambda x: x.lower())
        users_df_aad.loc[(users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal') & (users_df_aad['userPrincipalName'] == 'none'), 'userPrincipalName'] = users_df_aad.loc[(users_df_aad['@odata.type'] =='#microsoft.graph.servicePrincipal') & (users_df_aad['userPrincipalName'] == 'none'), 'displayName']

        
        users_df_dbx.loc[users_df_dbx['type'] =='ServicePrincipal', 'userName'] = users_df_dbx.loc[users_df_dbx['type'] =='ServicePrincipal', 'displayName'].apply(lambda x: x.lower())

        users_df_aad['aad_group_displayName'] = users_df_aad['aad_group_displayName'].apply(lambda s: s.lower() if isinstance(s, str) else s)
        users_df_dbx['group_displayName'] = users_df_dbx['group_displayName'].apply(lambda s: s.lower() if isinstance(s, str) else s)

        net_delta = users_df_aad.merge(users_df_dbx, left_on=['aad_group_displayName', 'userPrincipalName'], 
                                       right_on=['group_displayName', 'userName'], how='outer')
        mappings_to_add = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna())]
        
        group_master_df = await self.get_all_groups_dbx()

        mappings_to_remove = net_delta[(net_delta['id_x'].isna()) & (net_delta['id_y'].notna()) & (net_delta['group_displayName'].notna())]
        mappings_to_remove = mappings_to_remove[mappings_to_remove['group_displayName'].isin([x.lower() for x in self.groups_to_sync])]
        
        if self.is_dryrun:
            print('This is a dry run')
            mappings_to_remove.to_csv(f"{self.log_file_dir}mappings_to_remove_Users.csv")
            mappings_to_add.to_csv(f"{self.log_file_dir}mappings_to_add_Users.csv")
        print(f" Total Mappings for Users to be removed: {mappings_to_remove.shape[0]}") 
        print(f" Total Mappings for Users to be added: {mappings_to_add.shape[0]}") 
        mappings_to_remove = mappings_to_remove[['id_y', 'group_id_y']].drop_duplicates()

        if not self.is_dryrun:
            mappings_to_remove.to_csv(f"{self.log_file_dir}mappings_to_remove_Users.csv")
            mappings_to_add.to_csv(f"{self.log_file_dir}mappings_to_add_Users.csv")
            await self.remove_dbx_group_mappings(mappings_to_remove)
            await self.add_dbx_group_mappings(mappings_to_add, group_master_df, users_df_dbx)

        # if 'appId' in users_df_dbx.columns:
        #     net_delta_spns = users_df_aad.merge(users_df_dbx, left_on=['group_id', 'appId'], 
        #                                         right_on=['group_externalId', 'applicationId'], how='outer')
        #     net_delta_spns = net_delta_spns[net_delta_spns['@odata.type'] == '#microsoft.graph.servicePrincipal']
        #     mappings_to_add_spns = net_delta_spns[(net_delta_spns['id_x'].notna()) & (net_delta_spns['id_y'].isna())]
            
        #     mappings_to_remove_spns = net_delta_spns[(net_delta_spns['id_x'].isna()) & (net_delta_spns['id_y'].notna()) & (net_delta_spns['group_externalId'].notna())]
        #     mappings_to_remove_spns = mappings_to_remove_spns[mappings_to_remove_spns['group_displayName'].isin(self.groups_to_sync)]
        #     if self.is_dryrun:
        #         print('This is a dry run')
        #         print(" Total Mappings for SPN's to be removed:")
        #         mappings_to_remove_spns.to_csv(f"{self.log_file_dir}mappings_to_remove_SPNs.csv")
        #         mappings_to_add_spns.to_csv(f"{self.log_file_dir}mappings_to_add_SPNs.csv")
        #     mappings_to_remove_spns = mappings_to_remove_spns[['id_y', 'group_id_y']].drop_duplicates()   
        #     print(f" Total New Mappings for SPNs: {mappings_to_add_spns.shape[0]}")
        #     print(f" Total Mappings for SPNs to be removed: {mappings_to_remove_spns.shape[0]}") 

        #     if not self.is_dryrun:
        #         await self.remove_dbx_group_mappings(mappings_to_remove_spns)
        #         await self.add_dbx_group_mappings(mappings_to_add_spns, group_master_df, users_df_dbx)
        #         mappings_to_remove_spns.to_csv(f"{self.log_file_dir}mappings_to_remove_SPNs.csv")
        #         mappings_to_add_spns.to_csv(f"{self.log_file_dir}mappings_to_add_SPNs.csv")


    async def deactivate_deleted_users(self):
        """
        Deactivate users in Databricks that have been deleted from Azure AD asynchronously.

        This function identifies users that exist in Databricks but have been deleted from Azure AD,
        and deactivates them in Databricks.

        Notes:
            - Fetches deleted users from Azure AD and compares with active users in Databricks.
            - Excludes admin users from deactivation for safety.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the deactivation process and results.
            - Implements asynchronous operations for improved performance.
        """
        all_users_dbx_df = await self.get_all_users_dbx()
        all_spns_dbx_df = await self.get_all_spns_dbx()
        unique_usernames_dbx = all_users_dbx_df['userName'].unique()
        unique_spns_dbx = all_spns_dbx_df['displayName'].unique()
        unique_usernames_dbx = [x.lower() for x in unique_usernames_dbx]
        unique_spns_dbx = [str(x).lower() for x in unique_spns_dbx]

        all_users_aad_df = await self.get_users_by_username_aad(unique_usernames_dbx)
        all_spns_aad_df = await self.get_apps_by_displayName_aad(unique_spns_dbx)

        unique_usernames_aad = all_users_aad_df['userPrincipalName'].unique()
        unique_spns_aad = all_spns_aad_df['displayName'].unique()
        unique_usernames_aad = [x.lower() for x in unique_usernames_aad]
        unique_spns_aad = [x.lower() for x in unique_spns_aad]

        users_to_be_removed = list(set(unique_usernames_dbx) - set(unique_usernames_aad))
        apps_to_be_removed = list(set(unique_spns_dbx) - set(unique_spns_aad))
        ids_to_be_removed = []
        ids_to_be_removed.extend(list(all_users_dbx_df[all_users_dbx_df['userName'].isin(users_to_be_removed)]['id']))
        ids_to_be_removed.extend(list(all_spns_dbx_df[all_spns_dbx_df['displayName'].isin(apps_to_be_removed)]['id']))

        df_ids_to_be_removed = pd.DataFrame({'id_y': ids_to_be_removed})
        if not self.is_dryrun:
            await self.deactivate_users_dbx(df_ids_to_be_removed)
        else:
            if df_ids_to_be_removed.shape[0] > 0:
                print('Exporting Deactivation list for deleted users')
                df_ids_to_be_removed.to_csv(f"{self.log_file_dir}dbx_deleted_users_dump.csv")
        
        print(f" Total Deleted Users detected: {len(df_ids_to_be_removed)}")

    async def deactivate_orphan_users(self):
        """
        Deactivate orphan users in Databricks asynchronously.

        This function identifies users that exist in Databricks but are not present in any of the 
        synced Azure AD groups, and deactivates them in Databricks.

        Notes:
            - Compares users in Databricks with users in synced Azure AD groups.
            - Excludes users that belong to groups not in the sync list and account users.
            - Excludes admin users and groups from deactivation for safety.
            - Respects the is_dryrun flag to simulate or perform actual changes.
            - Logs the deactivation process and results.
            - Implements asynchronous operations for improved performance.
        """
        users_df_dbx = await self.get_all_user_groups_dbx(True)
        users_df_dbx = users_df_dbx[users_df_dbx['group_displayName'] != 'account users']
        spns_df_dbx = users_df_dbx[users_df_dbx['applicationId'].notna()]
        users_df_dbx = users_df_dbx[users_df_dbx['applicationId'].isna()]
        users_df_dbx['userName'] = users_df_dbx['userName'].apply(lambda s: s.lower() if isinstance(s, str) else s)

        all_users = await self.get_all_users_dbx()

        net_delta = all_users.merge(users_df_dbx, left_on=['userName'], right_on=['userName'], how='left')

        users_to_remove = net_delta[(net_delta['id_x'].notna()) & (net_delta['id_y'].isna()) & (net_delta['active_x'] == True)] 

        if self.is_dryrun:
            print('This is a dry run')
            if users_to_remove.shape[0] > 0:
                print('Exporting Deactivation list')
                users_to_remove.to_csv(f"{self.log_file_dir}dbx_orphan_users_dump.csv")
        print(f" Total Orphan Users: {users_to_remove[['id_x', 'isAdmin']].drop_duplicates().shape[0]}")

        users_to_remove = users_to_remove[['id_x', 'isAdmin']].drop_duplicates()
        users_to_remove.columns = ['id_y', 'isAdmin']

        if not self.is_dryrun:
            if users_to_remove.shape[0] > 0:
                print('Exporting Deactivation list')
                users_to_remove.to_csv(f"{self.log_file_dir}dbx_orphan_users_dump.csv")
            self.logger.info(f"Deactivating Orphan Users: {len(users_to_remove)}") 
            await self.deactivate_users_dbx(users_to_remove)

    async def delete_users_dbx(self, users_to_delete: pd.DataFrame) -> List[Any]:
        """
        Delete users from Databricks asynchronously.

        This function deletes multiple users from Databricks based on the provided DataFrame.

        Args:
            users_to_delete (pd.DataFrame): DataFrame containing user information to be deleted from Databricks.

        Returns:
            List[Any]: A list of results from user deletion operations.

        Notes:
            - Uses multi-threading to process multiple user deletions concurrently.
            - Implements retry mechanism and rate limiting for API calls.
            - Implements asynchronous operations for improved performance.
        """
        async def delete_user(url: str) -> Any:
            token = await self.get_dbx_token()
            headers = {'Authorization': f'Bearer {token}'}
            for retry in range(6):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.delete(url, headers=headers) as response:
                            if response.status == 204:
                                return await response.read()
                            elif response.status != 429:
                                response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    if e.status != 429 or retry == 5:
                        self.logger.error(f"Failed to delete user. URL: {url}, Error: {str(e)}")
                        return None
                await asyncio.sleep(2 ** retry)
            return None

        tasks = []
        for _, row in users_to_delete.iterrows():
            account_id = self.dbx_config.account_id
            url = f"{self.dbx_config.dbx_account_host}/api/2.0/accounts/{account_id}/scim/v2/Users/{row['id']}"
            tasks.append(delete_user(url))

        return await asyncio.gather(*tasks)


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
