import azure.functions as func
import json
import logging
import requests
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
import base64

app = func.FunctionApp()

@app.route(route="get_product_data", auth_level=func.AuthLevel.FUNCTION)
def get_product_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    BigCommerce Product Data Downloader
    Downloads product catalog data from BigCommerce REST API
    """
    logging.info('BigCommerce product data download function processed a request.')

    try:
        # Get parameters from request
        base_url = req.params.get('base_url')  # e.g., api.bigcommerce.com/stores/lb2chb77ok
        api_version = req.params.get('api_version', 'v3')  # Default to v3
        item = req.params.get('item', 'products')  # Default to products
        page_size = req.params.get('page_size', '250')
        auth_token = req.params.get('auth_token')  # BigCommerce API token
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'RetailProducts/input/files/json/products/base')
        filename_prefix = req.params.get('filename', 'bigcommerce')

        # Validate required parameters
        if not base_url:
            return func.HttpResponse(
                json.dumps({"error": "base_url parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "auth_token parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "datalake_key parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Construct full BigCommerce API URL from store hash
        if not base_url.startswith('http'):
            base_url = f"https://api.bigcommerce.com/stores/{base_url}"

        logging.info(f"Processing BigCommerce {item} data")
        logging.info(f"Base URL: {base_url}, API Version: {api_version}")
        logging.info(f"Page size: {page_size}")

        # Step 1: Fetch BigCommerce data
        catalog_data = fetch_bigcommerce_catalog_data(
            auth_token=auth_token,
            base_url=base_url,
            api_version=api_version,
            item=item,
            page_size=page_size
        )

        if not catalog_data:
            return func.HttpResponse(
                json.dumps({
                    "error": "Failed to fetch BigCommerce catalog data",
                    "debug": {
                        "base_url": base_url,
                        "api_version": api_version,
                        "item": item,
                        "auth_token_provided": bool(auth_token)
                    }
                }),
                status_code=500,
                mimetype="application/json"
            )

        # Step 2: Only save to Data Lake if we have actual data (no errors, no zero records)
        items_list = catalog_data.get('data', [])
        has_items = len(items_list) > 0
        has_errors = 'error' in catalog_data

        if has_errors:
            # Return error without saving any file
            return func.HttpResponse(
                json.dumps(catalog_data),
                status_code=500,
                mimetype="application/json"
            )

        if not has_items:
            # Create empty file when no items found (but no errors)
            empty_data = {
                "data": [],
                "total_count": 0,
                "metadata": {
                    "source": base_url,
                    "api_version": api_version,
                    "item_type": item,
                    "date_range": "all",
                    "page_size": page_size,
                    "note": f"No {item} found for this store"
                }
            }
            
            # Create filename for empty file
            filename = f"{filename_prefix}-{item}"
            
            # Save empty file to Data Lake
            save_result = save_to_datalake(empty_data, datalake_key, data_lake_path, filename)
            
            if save_result:
                response_data = {
                    "status": "success",
                    "message": f"No {item} found for this store - empty file created",
                    "records_count": 0,
                    "filename": f"{filename}.json",
                    "path": data_lake_path,
                    "note": f"Empty file created to indicate endpoint was successfully checked"
                }
            else:
                response_data = {
                    "status": "error",
                    "message": f"No {item} found and failed to create empty file",
                    "records_count": 0,
                    "filename": None,
                    "path": None,
                    "note": "Data Lake save failed"
                }
            
            return func.HttpResponse(
                json.dumps(response_data),
                status_code=200,
                mimetype="application/json"
            )

        # Only save if we have actual data
        # Simple filename format to match Magento pattern (no date)
        filename = f"{filename_prefix}-{item}"

        save_result = save_to_datalake(catalog_data, datalake_key, data_lake_path, filename)

        if save_result:
            response_data = {
                "status": "success",
                "message": f"Successfully downloaded and saved {item} data",
                "records_count": len(items_list),
                "filename": f"{filename}.json",
                "path": data_lake_path
            }
            return func.HttpResponse(
                json.dumps(response_data),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Failed to save data to Data Lake"}),
                status_code=500,
                mimetype="application/json"
            )

    except Exception as e:
        logging.error(f"Error in get_product_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )


def fetch_bigcommerce_catalog_data(auth_token: str, base_url: str, api_version: str, item: str, page_size: str) -> dict:
    """
    Fetch catalog data from BigCommerce REST API with pagination
    """
    try:
        logging.info(f"Starting BigCommerce {item} data fetch...")
        
        # Construct API URL based on item type
        if item in ['products', 'brands', 'categories']:
            url = f"{base_url}/{api_version}/catalog/{item}"
        elif item == 'trees':
            url = f"{base_url}/{api_version}/catalog/trees"
        elif item in ['variants', 'options', 'images']:
            # These require product IDs, so we'll fetch all products first, then get their variants/options/images
            url = f"{base_url}/{api_version}/catalog/products"
            logging.info(f"Fetching {item} requires product IDs, starting with products endpoint")
        else:
            url = f"{base_url}/{api_version}/catalog/{item}"
        
        logging.info(f"API URL: {url}")

        # Set up headers
        headers = {
            'X-Auth-Token': auth_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Set up parameters
        params = {
            'limit': page_size
        }

        # No date filtering for catalog data - get all current items

        logging.info(f"Request parameters: {params}")

        # Handle different endpoint types
        if item in ['variants', 'options', 'images']:
            # For product-specific endpoints, first get all products, then fetch their variants/options/images
            try:
                all_items = fetch_product_specific_data(headers, base_url, api_version, item, page_size)
            except Exception as e:
                logging.error(f"Error fetching product-specific data for {item}: {str(e)}")
                # Return partial success with error info
                return {
                    "error": "PARTIAL_FETCH_ERROR",
                    "message": f"Network issues encountered while fetching {item} data. Some data may be missing.",
                    "details": str(e),
                    "data": [],
                    "total_count": 0
                }
        else:
            # For direct catalog endpoints (products, brands, categories, category-trees)
            all_items = fetch_direct_catalog_data(url, headers, params, item)
        
        if isinstance(all_items, dict) and 'error' in all_items:
            return all_items

        logging.info(f"Completed fetching {item} data. Total items: {len(all_items)}")

        # Return data in consistent format
        return {
            "data": all_items,
            "total_count": len(all_items),
            "metadata": {
                "source": base_url,
                "api_version": api_version,
                "item_type": item,
                "date_range": "all",
                "fetch_timestamp": datetime.now().isoformat(),
                "pages_fetched": "multiple" if len(all_items) > 0 else 0
            }
        }

    except Exception as e:
        logging.error(f"Error fetching BigCommerce {item} data: {str(e)}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "error": "FETCH_ERROR",
            "message": str(e),
            "traceback": traceback.format_exc()
        }


def fetch_direct_catalog_data(url: str, headers: dict, params: dict, item: str) -> list:
    """
    Fetch data from direct catalog endpoints (products, brands, categories, category-trees)
    """
    all_items = []
    page = 1
    max_pages = 50  # Safety limit
    
    while page <= max_pages:
        current_params = params.copy()
        current_params['page'] = page
        
        logging.info(f"Fetching {item} page {page}...")
        
        response = requests.get(url, headers=headers, params=current_params, timeout=30)
        
        if response.status_code != 200:
            logging.error(f"API request failed with status {response.status_code}")
            logging.error(f"Response: {response.text}")
            return {
                "error": "API_CALL_FAILED",
                "status_code": response.status_code,
                "response_text": response.text,
                "request_url": url,
                "request_params": current_params
            }
        
        try:
            data = response.json()
            items = data.get('data', [])
            
            if not items:
                logging.info(f"No more {item} found on page {page}, stopping pagination")
                break
            
            all_items.extend(items)
            logging.info(f"Page {page}: Found {len(items)} {item}, total so far: {len(all_items)}")
            
            # Check if we have more pages
            meta = data.get('meta', {})
            pagination = meta.get('pagination', {})
            current_page = pagination.get('current_page', page)
            total_pages = pagination.get('total_pages', 1)
            
            if current_page >= total_pages:
                logging.info(f"Reached last page ({total_pages}), stopping pagination")
                break
            
            page += 1
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {e}")
            return {
                "error": "INVALID_JSON_RESPONSE",
                "response_text": response.text[:1000]  # First 1000 chars
            }
    
    return all_items


def fetch_product_specific_data(headers: dict, base_url: str, api_version: str, item: str, page_size: str) -> list:
    """
    Fetch product-specific data (variants, options, images) by first getting all products
    """
    logging.info(f"Fetching {item} data requires product IDs, first getting all products...")
    
    # First, get all products
    products_url = f"{base_url}/{api_version}/catalog/products"
    products_params = {'limit': page_size}
    
    all_products = fetch_direct_catalog_data(products_url, headers, products_params, "products")
    
    if isinstance(all_products, dict) and 'error' in all_products:
        return all_products
    
    if not all_products:
        logging.info("No products found, cannot fetch product-specific data")
        return []
    
    logging.info(f"Found {len(all_products)} products, now fetching {item} for each...")
    
    all_items = []
    
    for i, product in enumerate(all_products):
        product_id = product.get('id')
        if not product_id:
            continue
            
        logging.info(f"Fetching {item} for product {product_id} ({i+1}/{len(all_products)})")
        
        # Construct URL for product-specific endpoint
        if item == 'variants':
            endpoint_url = f"{base_url}/{api_version}/catalog/products/{product_id}/variants"
        elif item == 'options':
            endpoint_url = f"{base_url}/{api_version}/catalog/products/{product_id}/options"
        elif item == 'images':
            endpoint_url = f"{base_url}/{api_version}/catalog/products/{product_id}/images"
        
        # Fetch data for this product with retry logic for network issues
        max_retries = 3
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                response = requests.get(endpoint_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        items = data.get('data', [])
                        
                        # Add product_id to each item for reference
                        for item_data in items:
                            item_data['product_id'] = product_id
                        
                        all_items.extend(items)
                        logging.info(f"Found {len(items)} {item} for product {product_id}")
                        success = True
                        
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse JSON for product {product_id}: {e}")
                        break  # Don't retry JSON errors
                else:
                    logging.warning(f"Failed to fetch {item} for product {product_id}: {response.status_code}")
                    if response.status_code == 404:
                        break  # Don't retry 404s
                    retry_count += 1
                    if retry_count < max_retries:
                        logging.info(f"Retrying {item} for product {product_id} (attempt {retry_count + 1})")
                        
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                retry_count += 1
                logging.warning(f"Network error fetching {item} for product {product_id} (attempt {retry_count}): {str(e)}")
                if retry_count < max_retries:
                    logging.info(f"Retrying {item} for product {product_id} (attempt {retry_count + 1})")
                    import time
                    time.sleep(2)  # Wait 2 seconds before retry
                else:
                    logging.error(f"Max retries exceeded for {item} for product {product_id}")
        
        if not success and retry_count >= max_retries:
            logging.warning(f"Skipping {item} for product {product_id} after {max_retries} failed attempts")
    
    logging.info(f"Completed fetching {item}. Total items: {len(all_items)}")
    return all_items


def save_to_datalake(data: dict, datalake_key: str, path: str, filename: str = None) -> bool:
    """
    Save data to Azure Data Lake Storage (same config as Magento)
    """
    try:
        logging.info(f"Starting Data Lake save. Path: {path}, Filename: {filename}")
        logging.info(f"Data size: {len(str(data))} characters")
        
        # Initialize Data Lake client (using same config as Magento function)
        logging.info("Initializing Data Lake client...")
        account_name = "prodbimanager"
        account_url = f"https://{account_name}.dfs.core.windows.net"
        
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
            filename = f"bigcommerce_catalog.{timestamp}-products"
        
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
        logging.error(f"Error type: {type(e).__name__}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return False
