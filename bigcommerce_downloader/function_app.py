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


@app.route(route="get_status_data", auth_level=func.AuthLevel.FUNCTION)
def get_status_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    BigCommerce Order Status Data Downloader
    Downloads comprehensive order status information including fulfillment progress
    """
    logging.info('BigCommerce status data download function processed a request.')

    try:
        # Get parameters from request
        base_url = req.params.get('base_url')  # e.g., api.bigcommerce.com/stores/lb2chb77ok
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'Retail/BigCommerce/Status')
        filename_prefix = req.params.get('filename', 'bigcommerce-status')
        
        # Date filters
        min_date_created = req.params.get('min_date_created')
        max_date_created = req.params.get('max_date_created')
        
        # Status type filter
        status_type = req.params.get('status_type', 'comprehensive')  # comprehensive, orders_only, fulfillment_only

        # Validate required parameters
        if not all([base_url, auth_token, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "base_url, auth_token, and datalake_key are required"}),
                status_code=400,
                mimetype="application/json"
            )

        if not base_url.startswith('http'):
            base_url = f"https://api.bigcommerce.com/stores/{base_url}"

        logging.info(f"Processing BigCommerce status data - type: {status_type}")

        # Fetch comprehensive status data
        status_data = fetch_comprehensive_status_data(
            auth_token=auth_token,
            base_url=base_url,
            status_type=status_type,
            min_date_created=min_date_created,
            max_date_created=max_date_created
        )

        if 'error' in status_data:
            return func.HttpResponse(
                json.dumps(status_data),
                status_code=500,
                mimetype="application/json"
            )

        items_list = status_data.get('data', [])
        has_items = len(items_list) > 0

        if not has_items:
            return func.HttpResponse(
                json.dumps({"status": "success", "message": f"No status data found.", "records_count": 0}),
                status_code=200,
                mimetype="application/json"
            )

        # Filename based on status type
        filename = f"{filename_prefix}-{status_type}"

        save_result = save_to_datalake(status_data, datalake_key, data_lake_path, filename)

        if save_result:
            response_data = {
                "status": "success",
                "message": f"Successfully downloaded and saved {status_type} status data",
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
        logging.error(f"Error in get_status_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="get_order_data", auth_level=func.AuthLevel.FUNCTION)
def get_order_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    BigCommerce Order Data Downloader
    Downloads order data from BigCommerce REST API (v2)
    """
    logging.info('BigCommerce order data download function processed a request.')

    try:
        # Get parameters from request
        base_url = req.params.get('base_url')  # e.g., api.bigcommerce.com/stores/lb2chb77ok
        item = req.params.get('item', 'Orders')  # Default to Orders
        page_size = req.params.get('page_size', '250')
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'Retail/BigCommerce')
        filename_prefix = req.params.get('filename', 'bigcommerce')

        # Date filters
        min_date_created = req.params.get('min_date_created')
        max_date_created = req.params.get('max_date_created')

        # Validate required parameters
        if not all([base_url, auth_token, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "base_url, auth_token, and datalake_key are required"}),
                status_code=400,
                mimetype="application/json"
            )

        if not base_url.startswith('http'):
            base_url = f"https://api.bigcommerce.com/stores/{base_url}"

        logging.info(f"Processing BigCommerce {item} data")

        # Fetch BigCommerce order data
        order_data = fetch_bigcommerce_order_data(
            auth_token=auth_token,
            base_url=base_url,
            item=item,
            page_size=page_size,
            min_date_created=min_date_created,
            max_date_created=max_date_created
        )

        if 'error' in order_data:
            return func.HttpResponse(
                json.dumps(order_data),
                status_code=500,
                mimetype="application/json"
            )

        items_list = order_data.get('data', [])
        has_items = len(items_list) > 0

        if not has_items:
            return func.HttpResponse(
                json.dumps({"status": "success", "message": f"No new {item} found.", "records_count": 0}),
                status_code=200,
                mimetype="application/json"
            )

        # Filename based on item type
        filename = f"{filename_prefix}-{item}"

        save_result = save_to_datalake(order_data, datalake_key, data_lake_path, filename)

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
        logging.error(f"Error in get_order_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def fetch_bigcommerce_order_data(auth_token: str, base_url: str, item: str, page_size: str, min_date_created: str = None, max_date_created: str = None) -> dict:
    """
    Fetch order data from BigCommerce REST API (v2) with pagination.
    """
    try:
        logging.info(f"Starting BigCommerce {item} data fetch...")

        # Determine the correct API endpoint for the item
        endpoint_map = {
            'Orders': '/v2/orders',
            'OrderLine': '/v2/orders/products',
            'Fulfillments': '/v2/orders/shipments',
            'FulfillmentLines': None,  # This needs special handling
            'OrderStatuses': '/v2/order_statuses',  # Added for status tracking
            'OrderStatusHistory': None  # Special handling for status history
        }

        endpoint = endpoint_map.get(item)
        if item == 'FulfillmentLines':
            # FulfillmentLines are nested in Fulfillments, so we fetch fulfillments and extract them.
            all_items = fetch_fulfillment_lines(auth_token, base_url, page_size, min_date_created, max_date_created)
        elif item == 'OrderStatusHistory':
            # OrderStatusHistory requires special handling - fetch all orders then get status history for each
            all_items = fetch_order_status_history(auth_token, base_url, page_size, min_date_created, max_date_created)
        elif endpoint:
            all_items = fetch_v2_data(auth_token, base_url, endpoint, page_size, min_date_created, max_date_created)
        else:
            return {"error": "INVALID_ITEM", "message": f"The item '{item}' is not supported for order data."}

        if isinstance(all_items, dict) and 'error' in all_items:
            return all_items

        logging.info(f"Completed fetching {item} data. Total items: {len(all_items)}")

        return {
            "data": all_items,
            "total_count": len(all_items),
            "metadata": {
                "source": base_url,
                "api_version": "v2",
                "item_type": item,
                "fetch_timestamp": datetime.now().isoformat()
            }
        }

    except Exception as e:
        logging.error(f"Error fetching BigCommerce {item} data: {str(e)}")
        import traceback
        return {"error": "FETCH_ERROR", "message": str(e), "traceback": traceback.format_exc()}


def fetch_v2_data(auth_token: str, base_url: str, endpoint: str, page_size: str, min_date: str = None, max_date: str = None) -> list:
    """
    Generic fetcher for BigCommerce v2 endpoints with pagination.
    """
    all_items = []
    page = 1
    max_pages = 200  # Safety limit
    url = f"{base_url}{endpoint}"
    headers = {
        'X-Auth-Token': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    while page <= max_pages:
        params = {'limit': page_size, 'page': page}
        if min_date:
            params['min_date_created'] = min_date
        if max_date:
            params['max_date_created'] = max_date

        logging.info(f"Fetching page {page} from {endpoint}...")
        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 200:
            items = response.json()
            if not items:
                logging.info(f"No more items found on page {page}, stopping.")
                break
            all_items.extend(items)
            page += 1
        elif response.status_code == 204: # No content
            logging.info(f"Received 204 No Content on page {page}, stopping.")
            break
        else:
            logging.error(f"API request failed with status {response.status_code}: {response.text}")
            return {"error": "API_CALL_FAILED", "status_code": response.status_code, "details": response.text}

    return all_items


def fetch_fulfillment_lines(auth_token: str, base_url: str, page_size: str, min_date: str = None, max_date: str = None) -> list:
    """
    Fetches all fulfillments and extracts the line items from each.
    """
    logging.info("Fetching fulfillments to extract fulfillment lines...")
    fulfillments = fetch_v2_data(auth_token, base_url, '/v2/orders/shipments', page_size, min_date, max_date)

    if isinstance(fulfillments, dict) and 'error' in fulfillments:
        return fulfillments

    fulfillment_lines = []
    for fulfillment in fulfillments:
        # Each item in the 'items' array is a fulfillment line
        for line_item in fulfillment.get('items', []):
            # Add parent fulfillment info to the line item
            line_item['fulfillment_id'] = fulfillment.get('id')
            line_item['order_id'] = fulfillment.get('order_id')
            line_item['tracking_number'] = fulfillment.get('tracking_number')
            line_item['date_created'] = fulfillment.get('date_created')
            fulfillment_lines.append(line_item)
    
    return fulfillment_lines


def fetch_order_status_history(auth_token: str, base_url: str, page_size: str, min_date: str = None, max_date: str = None) -> list:
    """
    Fetches all orders and their status history to provide comprehensive status tracking.
    """
    logging.info("Fetching orders to extract status history...")
    orders = fetch_v2_data(auth_token, base_url, '/v2/orders', page_size, min_date, max_date)

    if isinstance(orders, dict) and 'error' in orders:
        return orders

    status_history = []
    headers = {
        'X-Auth-Token': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    for order in orders:
        order_id = order.get('id')
        if not order_id:
            continue

        # Create status history entry with enhanced status information
        status_entry = {
            'order_id': order_id,
            'current_status': order.get('status'),
            'current_status_id': order.get('status_id'),
            'custom_status': order.get('custom_status'),
            'payment_status': order.get('payment_status'),
            'date_created': order.get('date_created'),
            'date_modified': order.get('date_modified'),
            'date_shipped': order.get('date_shipped'),
            'items_total': order.get('items_total'),
            'items_shipped': order.get('items_shipped'),
            'fulfillment_progress': calculate_fulfillment_progress(order),
            'order_value': order.get('total_inc_tax'),
            'currency_code': order.get('currency_code')
        }
        
        status_history.append(status_entry)

    return status_history


def fetch_comprehensive_status_data(auth_token: str, base_url: str, status_type: str, min_date_created: str = None, max_date_created: str = None) -> dict:
    """
    Fetch comprehensive status data combining orders, fulfillments, and status tracking.
    """
    try:
        logging.info(f"Starting comprehensive status data fetch - type: {status_type}")
        
        all_status_data = []
        
        if status_type in ['comprehensive', 'orders_only']:
            # Fetch orders with enhanced status information
            orders = fetch_v2_data(auth_token, base_url, '/v2/orders', '250', min_date_created, max_date_created)
            if isinstance(orders, dict) and 'error' in orders:
                return orders
                
            logging.info(f"Fetched {len(orders)} orders for status analysis")
            
            # Enhance each order with status analysis
            for order in orders:
                enhanced_order = enhance_order_with_status(order)
                all_status_data.append(enhanced_order)
        
        if status_type in ['comprehensive', 'fulfillment_only']:
            # Fetch fulfillments with status information
            fulfillments = fetch_v2_data(auth_token, base_url, '/v2/orders/shipments', '250', min_date_created, max_date_created)
            if isinstance(fulfillments, dict) and 'error' in fulfillments:
                return fulfillments
                
            logging.info(f"Fetched {len(fulfillments)} fulfillments for status analysis")
            
            # Add fulfillment status data
            for fulfillment in fulfillments:
                enhanced_fulfillment = enhance_fulfillment_with_status(fulfillment)
                if status_type == 'fulfillment_only':
                    all_status_data.append(enhanced_fulfillment)
                else:
                    # Find corresponding order and merge fulfillment data
                    order_id = fulfillment.get('order_id')
                    for status_item in all_status_data:
                        if status_item.get('order_id') == order_id:
                            if 'fulfillments' not in status_item:
                                status_item['fulfillments'] = []
                            status_item['fulfillments'].append(enhanced_fulfillment)
                            break

        logging.info(f"Completed comprehensive status data fetch. Total items: {len(all_status_data)}")

        return {
            "data": all_status_data,
            "total_count": len(all_status_data),
            "metadata": {
                "source": base_url,
                "status_type": status_type,
                "api_version": "v2",
                "fetch_timestamp": datetime.now().isoformat(),
                "date_filters": {
                    "min_date_created": min_date_created,
                    "max_date_created": max_date_created
                }
            }
        }

    except Exception as e:
        logging.error(f"Error fetching comprehensive status data: {str(e)}")
        import traceback
        return {"error": "FETCH_ERROR", "message": str(e), "traceback": traceback.format_exc()}


def enhance_order_with_status(order: dict) -> dict:
    """
    Enhance order data with additional status analysis and tracking information.
    """
    enhanced_order = order.copy()
    
    # Add calculated status fields
    fulfillment_progress = calculate_fulfillment_progress(order)
    enhanced_order['fulfillment_progress'] = fulfillment_progress
    
    # Add payment status analysis
    payment_analysis = analyze_payment_status(order)
    enhanced_order['payment_analysis'] = payment_analysis
    
    # Add order lifecycle status
    lifecycle_status = determine_order_lifecycle_status(order)
    enhanced_order['lifecycle_status'] = lifecycle_status
    
    # Add time-based analysis
    time_analysis = analyze_order_timing(order)
    enhanced_order['time_analysis'] = time_analysis
    
    return enhanced_order


def enhance_fulfillment_with_status(fulfillment: dict) -> dict:
    """
    Enhance fulfillment data with additional status and tracking information.
    """
    enhanced_fulfillment = fulfillment.copy()
    
    # Add tracking status analysis
    tracking_analysis = analyze_tracking_status(fulfillment)
    enhanced_fulfillment['tracking_analysis'] = tracking_analysis
    
    # Add shipping timeline
    shipping_timeline = calculate_shipping_timeline(fulfillment)
    enhanced_fulfillment['shipping_timeline'] = shipping_timeline
    
    return enhanced_fulfillment


def analyze_payment_status(order: dict) -> dict:
    """
    Analyze payment status and provide detailed payment information.
    """
    payment_status = order.get('payment_status', '').lower()
    payment_method = order.get('payment_method', '')
    refunded_amount = float(order.get('refunded_amount', 0))
    total_amount = float(order.get('total_inc_tax', 0))
    
    analysis = {
        'payment_status': payment_status,
        'payment_method': payment_method,
        'is_paid': payment_status in ['captured', 'authorized', 'paid'],
        'is_refunded': refunded_amount > 0,
        'refund_percentage': round((refunded_amount / total_amount * 100), 2) if total_amount > 0 else 0,
        'payment_risk_level': 'low' if payment_status == 'captured' else 'medium' if payment_status == 'authorized' else 'high'
    }
    
    return analysis


def determine_order_lifecycle_status(order: dict) -> dict:
    """
    Determine the current lifecycle status of an order.
    """
    status = order.get('status', '').lower()
    items_total = order.get('items_total', 0)
    items_shipped = order.get('items_shipped', 0)
    payment_status = order.get('payment_status', '').lower()
    
    if status == 'incomplete':
        lifecycle = 'incomplete'
        stage = 'checkout'
    elif payment_status in ['declined', 'failed']:
        lifecycle = 'payment_failed'
        stage = 'payment'
    elif payment_status in ['pending', 'authorized']:
        lifecycle = 'payment_pending'
        stage = 'payment'
    elif items_shipped == 0:
        lifecycle = 'awaiting_fulfillment'
        stage = 'fulfillment'
    elif items_shipped < items_total:
        lifecycle = 'partially_fulfilled'
        stage = 'fulfillment'
    elif items_shipped >= items_total:
        lifecycle = 'fulfilled'
        stage = 'complete'
    else:
        lifecycle = 'unknown'
        stage = 'unknown'
    
    return {
        'lifecycle': lifecycle,
        'stage': stage,
        'is_complete': lifecycle == 'fulfilled',
        'requires_attention': lifecycle in ['incomplete', 'payment_failed', 'payment_pending']
    }


def analyze_order_timing(order: dict) -> dict:
    """
    Analyze timing aspects of the order lifecycle.
    """
    from datetime import datetime, timezone
    import dateutil.parser
    
    try:
        date_created = order.get('date_created')
        date_modified = order.get('date_modified')
        date_shipped = order.get('date_shipped')
        
        now = datetime.now(timezone.utc)
        
        timing = {
            'order_age_hours': None,
            'time_to_ship_hours': None,
            'last_modified_hours_ago': None,
            'is_stale': False
        }
        
        if date_created:
            created_dt = dateutil.parser.parse(date_created)
            timing['order_age_hours'] = round((now - created_dt).total_seconds() / 3600, 2)
            timing['is_stale'] = timing['order_age_hours'] > 168  # 7 days
            
        if date_shipped and date_created:
            created_dt = dateutil.parser.parse(date_created)
            shipped_dt = dateutil.parser.parse(date_shipped)
            timing['time_to_ship_hours'] = round((shipped_dt - created_dt).total_seconds() / 3600, 2)
            
        if date_modified:
            modified_dt = dateutil.parser.parse(date_modified)
            timing['last_modified_hours_ago'] = round((now - modified_dt).total_seconds() / 3600, 2)
            
        return timing
        
    except Exception as e:
        logging.warning(f"Error analyzing order timing: {e}")
        return {'error': 'timing_analysis_failed'}


def analyze_tracking_status(fulfillment: dict) -> dict:
    """
    Analyze tracking and shipping status information.
    """
    tracking_number = fulfillment.get('tracking_number', '')
    shipping_provider = fulfillment.get('shipping_provider', '').lower()
    tracking_link = fulfillment.get('generated_tracking_link', '')
    
    analysis = {
        'has_tracking': bool(tracking_number),
        'tracking_number': tracking_number,
        'shipping_provider': shipping_provider,
        'tracking_link': tracking_link,
        'provider_type': 'major_carrier' if shipping_provider in ['fedex', 'ups', 'usps', 'dhl'] else 'other',
        'tracking_available': bool(tracking_link)
    }
    
    return analysis


def calculate_shipping_timeline(fulfillment: dict) -> dict:
    """
    Calculate shipping timeline and delivery estimates.
    """
    from datetime import datetime, timezone
    import dateutil.parser
    
    try:
        date_created = fulfillment.get('date_created')
        date_shipped = fulfillment.get('date_shipped')
        
        timeline = {
            'fulfillment_age_hours': None,
            'processing_time_hours': None,
            'estimated_delivery_days': None
        }
        
        now = datetime.now(timezone.utc)
        
        if date_created:
            created_dt = dateutil.parser.parse(date_created)
            timeline['fulfillment_age_hours'] = round((now - created_dt).total_seconds() / 3600, 2)
            
        if date_shipped and date_created:
            created_dt = dateutil.parser.parse(date_created)
            shipped_dt = dateutil.parser.parse(date_shipped)
            timeline['processing_time_hours'] = round((shipped_dt - created_dt).total_seconds() / 3600, 2)
            
        # Estimate delivery based on shipping provider
        shipping_provider = fulfillment.get('shipping_provider', '').lower()
        if shipping_provider == 'fedex':
            timeline['estimated_delivery_days'] = 3
        elif shipping_provider == 'ups':
            timeline['estimated_delivery_days'] = 3
        elif shipping_provider == 'usps':
            timeline['estimated_delivery_days'] = 5
        else:
            timeline['estimated_delivery_days'] = 7
            
        return timeline
        
    except Exception as e:
        logging.warning(f"Error calculating shipping timeline: {e}")
        return {'error': 'timeline_calculation_failed'}


def calculate_fulfillment_progress(order: dict) -> dict:
    """
    Calculate fulfillment progress based on items shipped vs total items.
    """
    items_total = order.get('items_total', 0)
    items_shipped = order.get('items_shipped', 0)
    
    if items_total == 0:
        fulfillment_percentage = 0
        fulfillment_status = 'no_items'
    elif items_shipped == 0:
        fulfillment_percentage = 0
        fulfillment_status = 'pending'
    elif items_shipped >= items_total:
        fulfillment_percentage = 100
        fulfillment_status = 'complete'
    else:
        fulfillment_percentage = round((items_shipped / items_total) * 100, 2)
        fulfillment_status = 'partial'
    
    return {
        'fulfillment_percentage': fulfillment_percentage,
        'fulfillment_status': fulfillment_status,
        'items_total': items_total,
        'items_shipped': items_shipped,
        'items_pending': items_total - items_shipped
    }

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
