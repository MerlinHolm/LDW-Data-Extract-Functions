import azure.functions as func
import json
import logging
import asyncio
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

app = func.FunctionApp()

@app.route(route="sync_datalake", auth_level=func.AuthLevel.FUNCTION)
def sync_datalake(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Data Lake sync request')
    
    try:
        # Get required parameters
        source_account_name = req.params.get('source_account_name')
        source_account_key = req.params.get('source_account_key')
        source_container = req.params.get('source_container')
        
        target_account_name = req.params.get('target_account_name')
        target_account_key = req.params.get('target_account_key')
        target_container = req.params.get('target_container')
        
        # Optional parameters
        file_path = req.params.get('file_path', '')  # Optional: specific folder/file path
        overwrite = req.params.get('overwrite', 'false').lower() == 'true'
        dry_run = req.params.get('dry_run', 'false').lower() == 'true'
        
        # Validate required parameters
        required_params = [
            source_account_name, source_account_key, source_container,
            target_account_name, target_account_key, target_container
        ]
        
        if any(param is None for param in required_params):
            return func.HttpResponse(
                json.dumps({
                    "error": "Missing required parameters",
                    "required": [
                        "source_account_name", "source_account_key", "source_container",
                        "target_account_name", "target_account_key", "target_container"
                    ]
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Initialize Data Lake clients
        source_service_client = DataLakeServiceClient(
            account_url=f"https://{source_account_name}.dfs.core.windows.net",
            credential=source_account_key
        )
        
        target_service_client = DataLakeServiceClient(
            account_url=f"https://{target_account_name}.dfs.core.windows.net",
            credential=target_account_key
        )
        
        # Get file system clients
        source_file_system = source_service_client.get_file_system_client(source_container)
        target_file_system = target_service_client.get_file_system_client(target_container)
        
        # Ensure target container exists
        try:
            target_file_system.create_file_system()
            logging.info(f"Created target container: {target_container}")
        except ResourceExistsError:
            logging.info(f"Target container already exists: {target_container}")
        
        # Perform sync operation
        result = sync_files(
            source_file_system, 
            target_file_system, 
            file_path, 
            overwrite, 
            dry_run
        )
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error in sync_datalake: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def sync_files(source_fs, target_fs, file_path, overwrite, dry_run):
    """
    Sync files from source to target file system
    """
    files_copied = 0
    files_skipped = 0
    files_failed = 0
    total_size = 0
    errors = []
    
    try:
        # List files in source
        if file_path:
            # Specific path provided
            paths = source_fs.get_paths(path=file_path, recursive=True)
        else:
            # All files
            paths = source_fs.get_paths(recursive=True)
        
        for path in paths:
            try:
                # Skip directories
                if path.is_directory:
                    continue
                
                source_path = path.name
                logging.info(f"Processing file: {source_path}")
                
                # Check if target file exists
                target_exists = False
                try:
                    target_file_client = target_fs.get_file_client(source_path)
                    target_file_client.get_file_properties()
                    target_exists = True
                except ResourceNotFoundError:
                    target_exists = False
                
                # Skip if file exists and overwrite is False
                if target_exists and not overwrite:
                    logging.info(f"Skipping existing file: {source_path}")
                    files_skipped += 1
                    continue
                
                if dry_run:
                    logging.info(f"DRY RUN: Would copy {source_path}")
                    files_copied += 1
                    continue
                
                # Download from source
                source_file_client = source_fs.get_file_client(source_path)
                download = source_file_client.download_file()
                file_content = download.readall()
                
                # Create directory structure in target if needed
                directory_path = '/'.join(source_path.split('/')[:-1])
                if directory_path:
                    try:
                        target_fs.create_directory(directory_path)
                    except ResourceExistsError:
                        pass  # Directory already exists
                
                # Upload to target
                target_file_client = target_fs.get_file_client(source_path)
                target_file_client.upload_data(
                    file_content, 
                    overwrite=overwrite
                )
                
                files_copied += 1
                total_size += len(file_content)
                logging.info(f"Successfully copied: {source_path} ({len(file_content)} bytes)")
                
            except Exception as file_error:
                logging.error(f"Failed to copy {source_path}: {str(file_error)}")
                files_failed += 1
                errors.append({
                    "file": source_path,
                    "error": str(file_error)
                })
        
        return {
            "status": "completed",
            "summary": {
                "files_copied": files_copied,
                "files_skipped": files_skipped,
                "files_failed": files_failed,
                "total_size_bytes": total_size,
                "dry_run": dry_run
            },
            "source_path": file_path if file_path else "all files",
            "timestamp": datetime.utcnow().isoformat(),
            "errors": errors
        }
        
    except Exception as e:
        logging.error(f"Error in sync_files: {str(e)}")
        return {
            "status": "failed",
            "error": str(e),
            "summary": {
                "files_copied": files_copied,
                "files_skipped": files_skipped,
                "files_failed": files_failed,
                "total_size_bytes": total_size
            }
        }


@app.route(route="list_files", auth_level=func.AuthLevel.FUNCTION)
def list_files(req: func.HttpRequest) -> func.HttpResponse:
    """
    List files in a data lake location for inspection before sync
    """
    logging.info('Processing Data Lake list files request')
    
    try:
        # Get required parameters
        account_name = req.params.get('account_name')
        account_key = req.params.get('account_key')
        container = req.params.get('container')
        file_path = req.params.get('file_path', '')
        
        if not all([account_name, account_key, container]):
            return func.HttpResponse(
                json.dumps({
                    "error": "Missing required parameters",
                    "required": ["account_name", "account_key", "container"]
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Initialize Data Lake client
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )
        
        file_system = service_client.get_file_system_client(container)
        
        # List files
        files = []
        total_size = 0
        
        if file_path:
            paths = file_system.get_paths(path=file_path, recursive=True)
        else:
            paths = file_system.get_paths(recursive=True)
        
        for path in paths:
            if not path.is_directory:
                file_info = {
                    "name": path.name,
                    "size": path.content_length or 0,
                    "last_modified": path.last_modified.isoformat() if path.last_modified else None
                }
                files.append(file_info)
                total_size += file_info["size"]
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "container": container,
                "path": file_path if file_path else "all files",
                "file_count": len(files),
                "total_size_bytes": total_size,
                "files": files[:100],  # Limit to first 100 files for response size
                "truncated": len(files) > 100
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error in list_files: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
