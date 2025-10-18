import azure.functions as func
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient
import io

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="get_channel_data")
def get_channel_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fetch Slack channel data (messages, users, etc.) and save to DataLake as parquet files.
    
    Required Parameters:
    - auth_token: Slack Bot User OAuth Token (xoxb-...)
    - datalake_key: Azure Data Lake connection string
    
    Optional Parameters:
    - channel: Channel ID or name (if not provided, fetches all channels)
    - workspace: Workspace/Team ID (for multi-workspace bots)
    - api_version: Slack API version (default: v1)
    - data_lake_path: Path in DataLake (default: 'Communication/Slack/Channels')
    - filename_prefix: Prefix for saved files (default: 'slack')
    - oldest: Oldest timestamp for messages (Unix timestamp)
    - latest: Latest timestamp for messages (Unix timestamp)
    - limit: Number of messages per request (default: 1000, max: 1000)
    - include_users: Include user information (default: true)
    - include_channel_info: Include channel metadata (default: true)
    """
    logging.info('Slack channel data download function processed a request.')

    try:
        # Get parameters from request
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        channel = req.params.get('channel')
        workspace = req.params.get('workspace')
        api_version = req.params.get('api_version', 'v1')
        data_lake_path = req.params.get('data_lake_path', 'Communication/Slack/Channels')
        filename_prefix = req.params.get('filename_prefix', 'slack')
        oldest = req.params.get('oldest')
        latest = req.params.get('latest')
        limit = int(req.params.get('limit', '1000'))
        include_users = req.params.get('include_users', 'true').lower() == 'true'
        include_channel_info = req.params.get('include_channel_info', 'true').lower() == 'true'

        # Validate required parameters
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token parameter is required (Slack Bot User OAuth Token)"}),
                status_code=400,
                mimetype="application/json"
            )

        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "datalake_key parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Validate limit
        if limit > 1000:
            limit = 1000

        logging.info(f"Fetching Slack data for channel: {channel or 'ALL'}")
        logging.info(f"Workspace: {workspace or 'DEFAULT'}")
        logging.info(f"API Version: {api_version}")
        logging.info(f"Data Lake path: {data_lake_path}")
        logging.info(f"Include users: {include_users}")
        logging.info(f"Include channel info: {include_channel_info}")

        # Fetch Slack data
        slack_data = fetch_slack_channel_data(
            auth_token, channel, workspace, oldest, latest, limit, 
            include_users, include_channel_info
        )

        # Check for errors
        if 'error' in slack_data:
            return func.HttpResponse(
                json.dumps(slack_data),
                status_code=500,
                mimetype="application/json"
            )

        # Save data to DataLake as parquet files
        save_results = []
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Save messages data
        if 'messages' in slack_data and slack_data['messages']:
            messages_filename = f"{filename_prefix}_messages_{timestamp}"
            if channel:
                messages_filename = f"{filename_prefix}_messages_{channel}_{timestamp}"
            
            messages_result = save_to_datalake_parquet(
                slack_data['messages'], datalake_key, data_lake_path, messages_filename
            )
            save_results.append({"type": "messages", "result": messages_result})

        # Save channels data
        if 'channels' in slack_data and slack_data['channels']:
            channels_filename = f"{filename_prefix}_channels_{timestamp}"
            channels_result = save_to_datalake_parquet(
                slack_data['channels'], datalake_key, data_lake_path, channels_filename
            )
            save_results.append({"type": "channels", "result": channels_result})

        # Save users data
        if 'users' in slack_data and slack_data['users']:
            users_filename = f"{filename_prefix}_users_{timestamp}"
            users_result = save_to_datalake_parquet(
                slack_data['users'], datalake_key, data_lake_path, users_filename
            )
            save_results.append({"type": "users", "result": users_result})

        # Prepare response
        successful_saves = [r for r in save_results if r['result'].get('success')]
        failed_saves = [r for r in save_results if not r['result'].get('success')]

        response_data = {
            "status": "success",
            "message": f"Processed Slack data for {len(successful_saves)} data types",
            "channel": channel or "ALL_CHANNELS",
            "workspace": workspace or "DEFAULT",
            "files_saved": len(successful_saves),
            "files_failed": len(failed_saves),
            "save_results": save_results,
            "path": data_lake_path,
            "timestamp": timestamp
        }

        if slack_data.get('metadata'):
            response_data['metadata'] = slack_data['metadata']

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error in get_channel_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "INTERNAL_ERROR", "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="get_workspace_data")
def get_workspace_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fetch Slack workspace data (team info, users, channels list) and save to DataLake as parquet files.
    
    Required Parameters:
    - auth_token: Slack Bot User OAuth Token (xoxb-...)
    - datalake_key: Azure Data Lake connection string
    
    Optional Parameters:
    - workspace: Workspace/Team ID (for multi-workspace bots)
    - api_version: Slack API version (default: v1)
    - data_lake_path: Path in DataLake (default: 'Communication/Slack/Workspace')
    - filename_prefix: Prefix for saved files (default: 'slack_workspace')
    - include_archived: Include archived channels (default: false)
    - include_private: Include private channels (default: false)
    """
    logging.info('Slack workspace data download function processed a request.')

    try:
        # Get parameters from request
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        workspace = req.params.get('workspace')
        api_version = req.params.get('api_version', 'v1')
        data_lake_path = req.params.get('data_lake_path', 'Communication/Slack/Workspace')
        filename_prefix = req.params.get('filename_prefix', 'slack_workspace')
        include_archived = req.params.get('include_archived', 'false').lower() == 'true'
        include_private = req.params.get('include_private', 'false').lower() == 'true'

        # Validate required parameters
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token parameter is required (Slack Bot User OAuth Token)"}),
                status_code=400,
                mimetype="application/json"
            )

        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "datalake_key parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        logging.info(f"Fetching Slack workspace data")
        logging.info(f"Workspace: {workspace or 'DEFAULT'}")
        logging.info(f"Include archived: {include_archived}")
        logging.info(f"Include private: {include_private}")

        # Fetch Slack workspace data
        workspace_data = fetch_slack_workspace_data(
            auth_token, workspace, include_archived, include_private
        )

        # Check for errors
        if 'error' in workspace_data:
            return func.HttpResponse(
                json.dumps(workspace_data),
                status_code=500,
                mimetype="application/json"
            )

        # Save data to DataLake as parquet files
        save_results = []
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Save team info
        if 'team_info' in workspace_data and workspace_data['team_info']:
            team_filename = f"{filename_prefix}_team_info_{timestamp}"
            team_result = save_to_datalake_parquet(
                [workspace_data['team_info']], datalake_key, data_lake_path, team_filename
            )
            save_results.append({"type": "team_info", "result": team_result})

        # Save all channels
        if 'channels' in workspace_data and workspace_data['channels']:
            channels_filename = f"{filename_prefix}_channels_{timestamp}"
            channels_result = save_to_datalake_parquet(
                workspace_data['channels'], datalake_key, data_lake_path, channels_filename
            )
            save_results.append({"type": "channels", "result": channels_result})

        # Save all users
        if 'users' in workspace_data and workspace_data['users']:
            users_filename = f"{filename_prefix}_users_{timestamp}"
            users_result = save_to_datalake_parquet(
                workspace_data['users'], datalake_key, data_lake_path, users_filename
            )
            save_results.append({"type": "users", "result": users_result})

        # Prepare response
        successful_saves = [r for r in save_results if r['result'].get('success')]
        failed_saves = [r for r in save_results if not r['result'].get('success')]

        response_data = {
            "status": "success",
            "message": f"Processed Slack workspace data for {len(successful_saves)} data types",
            "workspace": workspace or "DEFAULT",
            "files_saved": len(successful_saves),
            "files_failed": len(failed_saves),
            "save_results": save_results,
            "path": data_lake_path,
            "timestamp": timestamp
        }

        if workspace_data.get('metadata'):
            response_data['metadata'] = workspace_data['metadata']

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error in get_workspace_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "INTERNAL_ERROR", "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def fetch_slack_channel_data(auth_token, channel=None, workspace=None, oldest=None, latest=None, 
                           limit=1000, include_users=True, include_channel_info=True):
    """
    Fetch Slack channel data using the Slack Web API.
    """
    try:
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
        
        base_url = 'https://slack.com/api'
        result_data = {}
        
        # Get channels list if no specific channel provided
        channels_to_process = []
        
        if channel:
            # Single channel - validate it exists
            if channel.startswith('#'):
                channel = channel[1:]  # Remove # prefix
            
            # Try to get channel info
            channel_info_url = f"{base_url}/conversations.info"
            channel_params = {'channel': channel}
            if workspace:
                channel_params['team'] = workspace
                
            channel_response = requests.get(channel_info_url, headers=headers, params=channel_params)
            channel_data = channel_response.json()
            
            if not channel_data.get('ok'):
                return {
                    "error": "CHANNEL_NOT_FOUND",
                    "message": f"Channel '{channel}' not found or not accessible",
                    "slack_error": channel_data.get('error', 'Unknown error')
                }
            
            channels_to_process = [channel_data['channel']]
        else:
            # Get all channels
            channels_url = f"{base_url}/conversations.list"
            channels_params = {
                'exclude_archived': 'false',
                'types': 'public_channel,private_channel',
                'limit': 1000
            }
            if workspace:
                channels_params['team'] = workspace
                
            channels_response = requests.get(channels_url, headers=headers, params=channels_params)
            channels_data = channels_response.json()
            
            if not channels_data.get('ok'):
                return {
                    "error": "CHANNELS_FETCH_ERROR",
                    "message": "Failed to fetch channels list",
                    "slack_error": channels_data.get('error', 'Unknown error')
                }
            
            channels_to_process = channels_data.get('channels', [])
        
        # Store channel info if requested
        if include_channel_info:
            result_data['channels'] = channels_to_process
        
        # Fetch messages for each channel
        all_messages = []
        
        for channel_info in channels_to_process:
            channel_id = channel_info['id']
            channel_name = channel_info.get('name', channel_id)
            
            logging.info(f"Fetching messages for channel: {channel_name} ({channel_id})")
            
            # Get channel messages
            messages_url = f"{base_url}/conversations.history"
            messages_params = {
                'channel': channel_id,
                'limit': limit
            }
            
            if oldest:
                messages_params['oldest'] = oldest
            if latest:
                messages_params['latest'] = latest
            if workspace:
                messages_params['team'] = workspace
            
            # Handle pagination
            cursor = None
            channel_messages = []
            
            while True:
                if cursor:
                    messages_params['cursor'] = cursor
                
                messages_response = requests.get(messages_url, headers=headers, params=messages_params)
                messages_data = messages_response.json()
                
                if not messages_data.get('ok'):
                    logging.warning(f"Failed to fetch messages for channel {channel_name}: {messages_data.get('error')}")
                    break
                
                messages = messages_data.get('messages', [])
                
                # Add channel context to each message
                for message in messages:
                    message['channel_id'] = channel_id
                    message['channel_name'] = channel_name
                    
                    # Convert timestamp to readable format
                    if 'ts' in message:
                        try:
                            message['timestamp_readable'] = datetime.fromtimestamp(
                                float(message['ts']), tz=timezone.utc
                            ).isoformat()
                        except:
                            pass
                
                channel_messages.extend(messages)
                
                # Check for more pages
                if not messages_data.get('has_more') or not messages_data.get('response_metadata', {}).get('next_cursor'):
                    break
                
                cursor = messages_data['response_metadata']['next_cursor']
            
            all_messages.extend(channel_messages)
            logging.info(f"Fetched {len(channel_messages)} messages from {channel_name}")
        
        result_data['messages'] = all_messages
        
        # Get users list if requested
        if include_users:
            users_url = f"{base_url}/users.list"
            users_params = {'limit': 1000}
            if workspace:
                users_params['team'] = workspace
            
            users_response = requests.get(users_url, headers=headers, params=users_params)
            users_data = users_response.json()
            
            if users_data.get('ok'):
                result_data['users'] = users_data.get('members', [])
            else:
                logging.warning(f"Failed to fetch users: {users_data.get('error')}")
        
        # Add metadata
        result_data['metadata'] = {
            'fetch_timestamp': datetime.now(timezone.utc).isoformat(),
            'channels_processed': len(channels_to_process),
            'total_messages': len(all_messages),
            'oldest_filter': oldest,
            'latest_filter': latest,
            'limit_per_request': limit,
            'workspace': workspace
        }
        
        return result_data
        
    except Exception as e:
        logging.error(f"Error fetching Slack channel data: {str(e)}")
        return {
            "error": "FETCH_ERROR",
            "message": f"Failed to fetch Slack data: {str(e)}"
        }


def fetch_slack_workspace_data(auth_token, workspace=None, include_archived=False, include_private=False):
    """
    Fetch Slack workspace data including team info, channels, and users.
    """
    try:
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
        
        base_url = 'https://slack.com/api'
        result_data = {}
        
        # Get team info
        team_url = f"{base_url}/team.info"
        team_params = {}
        if workspace:
            team_params['team'] = workspace
            
        team_response = requests.get(team_url, headers=headers, params=team_params)
        team_data = team_response.json()
        
        if team_data.get('ok'):
            result_data['team_info'] = team_data.get('team', {})
        else:
            logging.warning(f"Failed to fetch team info: {team_data.get('error')}")
        
        # Get all channels
        channels_url = f"{base_url}/conversations.list"
        channel_types = ['public_channel']
        if include_private:
            channel_types.append('private_channel')
        
        channels_params = {
            'exclude_archived': str(not include_archived).lower(),
            'types': ','.join(channel_types),
            'limit': 1000
        }
        if workspace:
            channels_params['team'] = workspace
        
        all_channels = []
        cursor = None
        
        while True:
            if cursor:
                channels_params['cursor'] = cursor
            
            channels_response = requests.get(channels_url, headers=headers, params=channels_params)
            channels_data = channels_response.json()
            
            if not channels_data.get('ok'):
                logging.warning(f"Failed to fetch channels: {channels_data.get('error')}")
                break
            
            channels = channels_data.get('channels', [])
            all_channels.extend(channels)
            
            # Check for more pages
            if not channels_data.get('response_metadata', {}).get('next_cursor'):
                break
            
            cursor = channels_data['response_metadata']['next_cursor']
        
        result_data['channels'] = all_channels
        
        # Get all users
        users_url = f"{base_url}/users.list"
        users_params = {'limit': 1000}
        if workspace:
            users_params['team'] = workspace
        
        all_users = []
        cursor = None
        
        while True:
            if cursor:
                users_params['cursor'] = cursor
            
            users_response = requests.get(users_url, headers=headers, params=users_params)
            users_data = users_response.json()
            
            if not users_data.get('ok'):
                logging.warning(f"Failed to fetch users: {users_data.get('error')}")
                break
            
            users = users_data.get('members', [])
            all_users.extend(users)
            
            # Check for more pages
            if not users_data.get('response_metadata', {}).get('next_cursor'):
                break
            
            cursor = users_data['response_metadata']['next_cursor']
        
        result_data['users'] = all_users
        
        # Add metadata
        result_data['metadata'] = {
            'fetch_timestamp': datetime.now(timezone.utc).isoformat(),
            'total_channels': len(all_channels),
            'total_users': len(all_users),
            'include_archived': include_archived,
            'include_private': include_private,
            'workspace': workspace
        }
        
        return result_data
        
    except Exception as e:
        logging.error(f"Error fetching Slack workspace data: {str(e)}")
        return {
            "error": "FETCH_ERROR",
            "message": f"Failed to fetch Slack workspace data: {str(e)}"
        }


def save_to_datalake_parquet(data, datalake_key, path, filename):
    """
    Save data to Azure Data Lake as a parquet file.
    """
    try:
        # Create Data Lake service client
        service_client = DataLakeServiceClient.from_connection_string(datalake_key)
        
        # Get filesystem (container) - using 'files' as default
        filesystem_name = 'files'
        filesystem_client = service_client.get_file_system_client(filesystem_name)
        
        # Ensure .parquet extension
        if not filename.endswith('.parquet'):
            filename = f"{filename}.parquet"
        
        # Full file path
        file_path = f"{path.strip('/')}/{filename}"
        
        # Convert data to DataFrame
        if isinstance(data, list) and len(data) > 0:
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            df = pd.json_normalize([data])
        else:
            # Empty data
            df = pd.DataFrame()
        
        # Convert DataFrame to parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_data = parquet_buffer.getvalue()
        
        # Create file and upload
        file_client = filesystem_client.get_file_client(file_path)
        file_client.upload_data(parquet_data, overwrite=True)
        
        logging.info(f"Successfully saved to Data Lake: {file_path}")
        
        return {
            "success": True,
            "path": file_path,
            "filesystem": filesystem_name,
            "size_bytes": len(parquet_data),
            "records_count": len(df)
        }
        
    except Exception as e:
        logging.error(f"Data Lake save error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }
