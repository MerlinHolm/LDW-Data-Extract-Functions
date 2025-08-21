import azure.functions as func
import logging
import json
import requests
import os
from urllib.parse import urlparse
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="get_board_data", methods=["GET", "POST"])
def get_board_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get parameters from request (support both GET and POST)
        if req.method == "GET":
            req_body = dict(req.params)
        else:
            req_body = req.get_json() or {}
        
        # Required parameters
        base_url = req_body.get('base_url', 'https://api.monday.com')
        api_version = req_body.get('api_version', 'v2')
        board_id = req_body.get('boardID', '9313119823')
        api_token = req_body.get('api_token')
        datalake_key = req_body.get('datalake_key')
        
        # Data lake details - use boardID for dynamic paths
        json_filename = req_body.get('json_filename', f'{board_id}.json')
        
        # Pipeline-compatible parameters
        file_system = req_body.get('fileSystem', 'prodbidlstorage')
        storage_path = req_body.get('storagePath', 'MondayBoards/input/files/')
        
        if not api_token:
            return func.HttpResponse(
                "Missing required parameter: api_token",
                status_code=400
            )
        
        if not datalake_key:
            return func.HttpResponse(
                "Missing required parameter: datalake_key",
                status_code=400
            )
        
        # GraphQL query - exact match to working query format
        query = {
            "query": f"query {{ boards (ids: {board_id}) {{ id name description columns {{ id title type }} items_page (limit: 500) {{ cursor items {{ id name column_values {{ id text type value ... on MirrorValue {{id display_value type}} }} subitems {{ id name column_values {{ id text type value ... on MirrorValue {{id display_value type}} }} }} }} }} }} }}"
        }
        
        # Headers for Monday.com API
        headers = {
            "Authorization": api_token,
            "Content-Type": "application/json"
        }
        
        # Initialize variables for error handling
        json_data = None
        api_error = None
        
        try:
            # Make API request to Monday.com
            api_url = f"{base_url}/{api_version}"
            logging.info(f"Making request to: {api_url}")
            logging.info(f"Query being sent: {query}")
            
            response = requests.post(api_url, json=query, headers=headers)
            response.raise_for_status()
            
            json_data = response.json()
            
            # Save to Data Lake - use boardID as filename with .json extension
            json_filename_with_ext = f"{board_id}.json"
            data_lake_path = f"{storage_path}json/boards"
            save_result = save_to_datalake(json_data, datalake_key, data_lake_path, json_filename_with_ext)
            
            if not save_result:
                api_error = "Failed to save to Data Lake"
            
            logging.info(f"JSON data saved to Data Lake: {data_lake_path}/{json_filename}")
            
        except Exception as e:
            api_error = str(e)
            logging.error(f"API request failed: {api_error}")
            
            # Create blank JSON data on error
            blank_data = {
                "error": api_error,
                "timestamp": datetime.now().isoformat(),
                "board_id": board_id,
                "data": None
            }
            
            # Save blank data to Data Lake - use boardID as filename with .json extension
            json_filename_with_ext = f"{board_id}.json"
            data_lake_path = f"{storage_path}json/boards"
            save_to_datalake(blank_data, datalake_key, data_lake_path, json_filename_with_ext)
            
            logging.info(f"Blank JSON data created due to error and saved to Data Lake")
        
        # Return response (success or error)
        json_filename_with_ext = f"{board_id}.json"
        data_lake_path = f"{storage_path}json/boards"
        if api_error:
            result = {
                'status': 'error',
                'message': f'API request failed: {api_error}',
                'error': api_error,
                'json_file': f"{data_lake_path}/{json_filename_with_ext}",
                'board_id': board_id,
                'file_system': file_system,
                'storage_path': storage_path,
                'data_lake_path': data_lake_path
            }
        else:
            result = {
                'status': 'success',
                'message': 'Board data downloaded successfully',
                'json_file': f"{data_lake_path}/{json_filename_with_ext}",
                'board_id': board_id,
                'file_system': file_system,
                'storage_path': storage_path,
                'data_lake_path': data_lake_path
            }
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Function error: {str(e)}")
        return func.HttpResponse(
            f"Function execution failed: {str(e)}",
            status_code=500
        )


@app.route(route="get_file_data", methods=["GET", "POST"])
def get_file_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get parameters from request (support both GET and POST)
        if req.method == "GET":
            req_body = dict(req.params)
        else:
            req_body = req.get_json() or {}
        
        # Check if this is a direct file URL download (like .NET version)
        file_url = req_body.get('fileUrl')
        if file_url:
            # Direct file download mode (matching .NET functionality)
            board_id = req_body.get('boardId', req_body.get('boardID', 'default'))
            file_system = req_body.get('fileSystemName', req_body.get('fileSystem', 'prodbidlstorage'))
            storage_path = req_body.get('storagePath', f'MondayBoards/input/files/csv/{board_id}/')
            file_name = req_body.get('fileName', extract_filename_from_url(file_url))
            datalake_key = req_body.get('datalake_key')
            
            if not datalake_key:
                return func.HttpResponse(
                    "Missing required parameter: datalake_key",
                    status_code=400
                )
            
            # Download file directly
            try:
                csv_response = requests.get(file_url)
                csv_response.raise_for_status()
                
                csv_content = csv_response.content.decode('utf-8')
                csv_data_lake_path = storage_path.rstrip('/')
                
                # Save to Data Lake
                csv_save_result = save_csv_to_datalake(csv_content, datalake_key, csv_data_lake_path, file_name)
                
                if csv_save_result:
                    result = {
                        'status': 'success',
                        'message': 'File downloaded and uploaded successfully',
                        'file_system': file_system,
                        'path': f"{csv_data_lake_path}/{file_name}",
                        'size': len(csv_content),
                        'board_id': board_id,
                        'file_name': file_name
                    }
                else:
                    result = {
                        'status': 'error',
                        'message': 'Failed to save file to Data Lake'
                    }
                
                return func.HttpResponse(
                    json.dumps(result),
                    status_code=200,
                    mimetype="application/json"
                )
                
            except Exception as e:
                return func.HttpResponse(
                    json.dumps({
                        'status': 'error',
                        'message': f'Failed to download file: {str(e)}'
                    }),
                    status_code=500,
                    mimetype="application/json"
                )
        
        # Original Monday.com API query mode
        board_id = req_body.get('boardID', '9313119823')
        api_token = req_body.get('api_token')
        datalake_key = req_body.get('datalake_key')
        column_id = req_body.get('column_id')  # Monday.com column ID containing files
        
        # Pipeline-compatible parameters
        file_system = req_body.get('fileSystem', 'prodbidlstorage')
        storage_path = req_body.get('storagePath', 'MondayBoards/input/files/')
        
        if not api_token:
            return func.HttpResponse(
                "The supplied code is not right",
                status_code=400
            )
        
        if not datalake_key:
            return func.HttpResponse(
                "Missing required parameter: datalake_key",
                status_code=400
            )
        
        
        # Fetch board data directly from Monday API
        try:
            base_url = req_body.get('base_url', 'https://api.monday.com')
            api_version = req_body.get('api_version', 'v2')
            
            query = {
                "query": f"query {{ boards (ids: {board_id}) {{ items_page (limit: 500) {{ items {{ id name assets {{ id name url public_url file_extension }} }} }} }} }}"
            }
            
            headers = {
                "Authorization": api_token,
                "Content-Type": "application/json"
            }
            
            api_url = f"{base_url}/{api_version}"
            logging.info(f"Fetching board data from: {api_url}")
            
            response = requests.post(api_url, json=query, headers=headers)
            response.raise_for_status()
            
            json_data = response.json()
            logging.info(f"Successfully fetched board data for board: {board_id}")
            logging.info(f"API Response: {json.dumps(json_data, indent=2)}")
            
        except Exception as e:
            return func.HttpResponse(
                f"Failed to fetch board data from Monday API: {str(e)}",
                status_code=400
            )
        
        # Extract and download CSV files from assets
        csv_files_downloaded = []
        
        logging.info(f"Looking for CXR CSV assets in board {board_id}")
        
        if 'data' in json_data and 'boards' in json_data['data'] and json_data['data']['boards']:
            board = json_data['data']['boards'][0]
            logging.info(f"Found board data: {board.get('id', 'unknown')}")
            
            if 'items_page' in board and 'items' in board['items_page']:
                items = board['items_page']['items']
                logging.info(f"Found {len(items)} items in board")
                
                for item in items:
                    item_id = item.get('id')
                    logging.info(f"Processing item {item_id}")
                    
                    # Process all items with assets
                    if 'assets' in item and item['assets']:
                        logging.info(f"Found {len(item['assets'])} assets in item {item_id}")
                        
                        for asset in item['assets']:
                            asset_id = asset.get('id')
                            asset_name = asset.get('name', 'unknown')
                            asset_url = asset.get('public_url')
                            file_extension = asset.get('file_extension', '')
                            
                            logging.info(f"Asset: {asset_name}, Extension: {file_extension}, URL: {asset_url}")
                            
                            # Filter: LOWER(LEFT(asset,3)) = 'cxr' AND file_extension = '.csv'
                            if (asset_name and len(asset_name) >= 3 and 
                                asset_name[:3].lower() == 'cxr' and 
                                file_extension == '.csv' and 
                                asset_url):
                                
                                logging.info(f"Found matching CXR CSV asset: {asset_name}")
                                
                                
                                # Construct CSV filename: itemID-assetID.csv (rowID-assetID)
                                csv_filename = f"{item_id}-{asset_id}.csv"
                                
                                # Download CSV file and save to Data Lake
                                try:
                                    csv_response = requests.get(asset_url)
                                    csv_response.raise_for_status()
                                    
                                    # Convert CSV content to string for Data Lake
                                    csv_content = csv_response.content.decode('utf-8')
                                    
                                    # Save to Data Lake
                                    csv_data_lake_path = f"{storage_path}csv/{board_id}"
                                    csv_save_result = save_csv_to_datalake(csv_content, datalake_key, csv_data_lake_path, csv_filename)
                                    
                                    if csv_save_result:
                                        csv_files_downloaded.append({
                                            'filename': csv_filename,
                                            'path': f"{csv_data_lake_path}/{csv_filename}",
                                            'data_lake_path': csv_data_lake_path,
                                            'asset_id': asset_id,
                                            'asset_name': asset_name,
                                            'item_name': item.get('name'),
                                            'item_id': item_id,
                                            'asset_url': asset_url,
                                            'file_system': file_system,
                                            'storage_path': storage_path
                                        })
                                        
                                        logging.info(f"CSV file saved to Data Lake: {csv_data_lake_path}/{csv_filename}")
                                    else:
                                        logging.error(f"Failed to save CSV file to Data Lake: {csv_filename}")
                                
                                except Exception as e:
                                    logging.error(f"Error downloading CSV file {asset_url}: {str(e)}")
                            else:
                                logging.info(f"Asset {asset_name} does not match CXR CSV criteria")
                    else:
                        logging.info(f"No assets found in item {item_id}")
        
        # Return success response
        result = {
            'status': 'success',
            'message': 'CSV files downloaded successfully',
            'csv_files_downloaded': len(csv_files_downloaded),
            'board_id': board_id
        }
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {str(e)}")
        return func.HttpResponse(
            f"API request failed: {str(e)}",
            status_code=500
        )
    except Exception as e:
        logging.error(f"Function error: {str(e)}")
        return func.HttpResponse(
            f"Function execution failed: {str(e)}",
            status_code=500
        )


def save_to_datalake(data: dict, datalake_key: str, path: str, filename: str = None) -> bool:
    """
    Save data to Azure Data Lake Storage
    """
    try:
        logging.info("Starting Data Lake save operation...")
        
        # Data Lake configuration
        account_name = "prodbimanager"
        account_url = f"https://{account_name}.dfs.core.windows.net"
        
        logging.info(f"Connecting to Data Lake account: {account_name}")
        
        service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=datalake_key
        )
        
        logging.info("Getting file system client...")
        filesystem_name = "prodbidlstorage"
        file_system_client = service_client.get_file_system_client(filesystem_name)
        
        # Use provided filename or generate one
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"monday_board.{timestamp}"
        
        # Ensure .json extension
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        # Full file path
        file_path = f"{path}/{filename}"
        logging.info(f"Full file path: {file_path}")
        
        # Convert data to JSON string
        logging.info("Converting data to JSON...")
        json_data = json.dumps(data, indent=2, default=str)
        logging.info(f"JSON data size: {len(json_data)} characters")
        
        # Upload to Data Lake
        logging.info("Getting file client and uploading...")
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(json_data, overwrite=True)
        
        logging.info(f"Successfully saved data to Data Lake: {file_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to Data Lake: {str(e)}")
        return False


def extract_filename_from_url(url: str) -> str:
    """
    Extract filename from URL, similar to .NET version
    """
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        path = parsed_url.path
        filename = path.split('/')[-1]
        
        if not filename:
            # Generate default filename with timestamp
            from datetime import datetime
            filename = f"monday_file_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return filename
    except:
        # Fallback filename
        from datetime import datetime
        return f"monday_file_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"


def save_csv_to_datalake(csv_content: str, datalake_key: str, path: str, filename: str) -> bool:
    """
    Save CSV content to Azure Data Lake Storage
    """
    try:
        logging.info("Starting CSV Data Lake save operation...")
        
        # Data Lake configuration
        account_name = "prodbimanager"
        account_url = f"https://{account_name}.dfs.core.windows.net"
        
        service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=datalake_key
        )
        
        filesystem_name = "prodbidlstorage"
        file_system_client = service_client.get_file_system_client(filesystem_name)
        
        # Full file path
        file_path = f"{path}/{filename}"
        logging.info(f"Full CSV file path: {file_path}")
        
        # Upload CSV to Data Lake
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(csv_content, overwrite=True)
        
        logging.info(f"Successfully saved CSV to Data Lake: {file_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving CSV to Data Lake: {str(e)}")
        return False
