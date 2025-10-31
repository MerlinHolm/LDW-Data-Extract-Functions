import azure.functions as func
import json
import logging
import requests
import re
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
        page_size = req.params.get('page_size', '100')  # Reduced for better rate limiting
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
                "pages_fetched": "multiple" if len(all_items) > 0 else 0,
                "variant_pagination_enabled": item == 'variants',
                "separate_endpoints": True,
                "enhanced_limits": True,
                "page_size_limit": 100
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
        
        # Fetch data for this product with pagination and retry logic
        product_items = fetch_paginated_product_data(endpoint_url, headers, product_id, item, page_size)
        
        if product_items:
            all_items.extend(product_items)
            logging.info(f"Found {len(product_items)} {item} for product {product_id}")
        else:
            logging.warning(f"No {item} found for product {product_id}")
    
    logging.info(f"Completed fetching {item}. Total items: {len(all_items)}")
    return all_items


def fetch_paginated_product_data(endpoint_url: str, headers: dict, product_id: str, item_type: str, page_size: str) -> list:
    """
    Fetch paginated data for a specific product (variants, options, images) with retry logic
    """
    all_items = []
    page = 1
    max_pages = 20  # Safety limit for product-specific pagination
    max_retries = 3
    
    # Add explicit limit parameter to ensure we get the requested page size
    limit = min(100, int(page_size))  # Cap at 100 for rate limiting
    
    while page <= max_pages:
        retry_count = 0
        success = False
        
        # Add pagination parameters
        paginated_url = f"{endpoint_url}?limit={limit}&page={page}"
        
        while retry_count < max_retries and not success:
            try:
                logging.info(f"Fetching {item_type} for product {product_id}, page {page} (limit: {limit})")
                response = requests.get(paginated_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        items = data.get('data', [])
                        
                        if not items:
                            logging.info(f"No more {item_type} found for product {product_id} on page {page}")
                            return all_items  # No more items, stop pagination
                        
                        # Add product_id to each item for reference
                        for item_data in items:
                            item_data['product_id'] = product_id
                        
                        all_items.extend(items)
                        logging.info(f"Page {page}: Found {len(items)} {item_type} for product {product_id}, total so far: {len(all_items)}")
                        
                        # Check if we got fewer items than requested (indicates last page)
                        if len(items) < limit:
                            logging.info(f"Got {len(items)} items (less than limit {limit}), assuming last page")
                            return all_items
                        
                        success = True
                        page += 1
                        break
                        
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse JSON for {item_type} product {product_id} page {page}: {e}")
                        return all_items  # Don't retry JSON errors
                        
                elif response.status_code == 404:
                    logging.info(f"404 for {item_type} product {product_id} page {page} - no more pages or no data")
                    return all_items  # Don't retry 404s
                    
                else:
                    logging.warning(f"Failed to fetch {item_type} for product {product_id} page {page}: {response.status_code}")
                    retry_count += 1
                    if retry_count < max_retries:
                        logging.info(f"Retrying {item_type} for product {product_id} page {page} (attempt {retry_count + 1})")
                        import time
                        time.sleep(2)  # Wait 2 seconds before retry
                    else:
                        logging.error(f"Max retries exceeded for {item_type} product {product_id} page {page}")
                        return all_items  # Return what we have so far
                        
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                retry_count += 1
                logging.warning(f"Network error fetching {item_type} for product {product_id} page {page} (attempt {retry_count}): {str(e)}")
                if retry_count < max_retries:
                    logging.info(f"Retrying {item_type} for product {product_id} page {page} (attempt {retry_count + 1})")
                    import time
                    time.sleep(2)  # Wait 2 seconds before retry
                else:
                    logging.error(f"Max retries exceeded for {item_type} product {product_id} page {page} due to network errors")
                    return all_items  # Return what we have so far
    
    logging.info(f"Reached max pages ({max_pages}) for {item_type} product {product_id}")
    return all_items


@app.route(route="get_status_data", auth_level=func.AuthLevel.FUNCTION)
def get_status_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    BigCommerce Order Status Data Downloader
    Downloads order status information matching Shopify function structure
    """
    logging.info('BigCommerce status data download function processed a request.')

    try:
        # Get parameters from request (matching Shopify structure)
        auth_token = req.params.get('auth_token')
        base_url = req.params.get('base_url')  # Store hash like 'lb2chb77ok'
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'Retail/BigCommerce/OrderStatus')
        page_size = req.params.get('page_size', '250')
        
        # BigCommerce equivalent of Shopify's order_number filter
        order_id_raw = req.params.get('order_id')
        order_id = None
        if order_id_raw:
            # Clean and validate order ID
            numeric_order_id = re.sub(r'\D', '', order_id_raw)
            if numeric_order_id:
                order_id = numeric_order_id
            else:
                return func.HttpResponse(
                    json.dumps({"status": "error", "message": "Invalid order_id parameter: must contain digits."}),
                    status_code=400, mimetype="application/json"
                )

        # Date filters (matching Shopify naming)
        created_at_min = req.params.get('created_at_min')
        created_at_max = req.params.get('created_at_max')
        updated_at_min = req.params.get('updated_at_min')
        updated_at_max = req.params.get('updated_at_max')

        # Validate required parameters (matching Shopify error format)
        if not all([auth_token, base_url, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token, base_url, and datalake_key are required"}),
                status_code=400, mimetype="application/json"
            )

        # Construct BigCommerce API URL
        if not base_url.startswith('http'):
            full_base_url = f"https://api.bigcommerce.com/stores/{base_url}"
        else:
            full_base_url = base_url
            
        logging.info(f"Fetching BigCommerce order status data from: {full_base_url}")

        # Fetch BigCommerce status data (equivalent to fetch_shopify_statuses)
        status_data = fetch_bigcommerce_statuses(auth_token, full_base_url, page_size, order_id, created_at_min, created_at_max, updated_at_min, updated_at_max)

        if 'error' in status_data:
            return func.HttpResponse(json.dumps(status_data), status_code=500, mimetype="application/json")

        orders = status_data.get('data', [])
        if not orders:
            # Include debugging information in the response when no orders found (matching Shopify)
            debug_info = {
                "status": "success", 
                "message": "No new order statuses found.", 
                "records_count": 0,
                "debug_info": {
                    "created_at_min": created_at_min,
                    "created_at_max": created_at_max,
                    "updated_at_min": updated_at_min,
                    "updated_at_max": updated_at_max,
                    "order_id": order_id,
                    "query_filter_used": status_data.get('query_filter_used', 'Not available'),
                    "total_pages_checked": status_data.get('total_pages_checked', 'Not available')
                }
            }
            return func.HttpResponse(json.dumps(debug_info), status_code=200, mimetype="application/json")

        saved_files = []
        failed_files = []
        for raw_order in orders:
            # Transform BigCommerce order data (equivalent to _transform_order)
            order = _transform_bigcommerce_order(raw_order)
            if not order:
                logging.warning("Skipping an order that failed transformation.")
                continue

            # Use order ID as filename (BigCommerce equivalent of Shopify's order name)
            order_id_for_filename = order.get('id')
            if order_id_for_filename:
                filename = str(order_id_for_filename)
                
                if filename:
                    if save_to_datalake(order, datalake_key, data_lake_path, filename):
                        saved_files.append(f"{filename}.json")
                    else:
                        failed_files.append(f"{filename}.json")
                else:
                    fallback_id = order.get('id', 'unknown_id')
                    logging.warning(f"Could not generate a valid filename from order ID: '{order_id_for_filename}'. Fallback ID: {fallback_id}")
                    failed_files.append(f"FAILED_INVALID_ID(id_{fallback_id})")
            else:
                fallback_id = order.get('id', 'unknown_id')
                logging.warning(f"Order is missing 'id' field. Cannot save file. Fallback ID: {fallback_id}")
                failed_files.append(f"FAILED_NO_ID(id_{fallback_id})")

        response_data = {
            "status": "partial_success" if failed_files else "success",
            "message": f"Processed {len(orders)} order statuses.",
            "records_saved": len(saved_files),
            "records_failed": len(failed_files),
            "failed_files": failed_files,
            "path": data_lake_path
        }
        
        return func.HttpResponse(json.dumps(response_data), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Unexpected error in get_status_data: {str(e)}")
        import traceback
        return func.HttpResponse(
            json.dumps({"error": "UNEXPECTED_ERROR", "message": str(e), "traceback": traceback.format_exc()}),
            status_code=500, mimetype="application/json"
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


def fetch_bigcommerce_statuses(auth_token: str, base_url: str, page_size: str, order_id: str = None, created_at_min: str = None, created_at_max: str = None, updated_at_min: str = None, updated_at_max: str = None) -> dict:
    """
    Fetch BigCommerce order status data (equivalent to fetch_shopify_statuses)
    """
    try:
        logging.info(f"Starting BigCommerce status data fetch...")
        
        # Build query parameters
        params = {'limit': page_size}
        
        # Add date filters if provided
        if created_at_min:
            params['min_date_created'] = created_at_min
        if created_at_max:
            params['max_date_created'] = created_at_max
        if updated_at_min:
            params['min_date_modified'] = updated_at_min
        if updated_at_max:
            params['max_date_modified'] = updated_at_max
            
        # Add order ID filter if provided
        query_filter_used = []
        if order_id:
            # For BigCommerce, we can filter by specific order ID
            endpoint = f"/v2/orders/{order_id}"
            query_filter_used.append(f"order_id:{order_id}")
        else:
            endpoint = "/v2/orders"
            if created_at_min:
                query_filter_used.append(f"created_at:>={created_at_min}")
            if created_at_max:
                query_filter_used.append(f"created_at:<={created_at_max}")
            if updated_at_min:
                query_filter_used.append(f"updated_at:>={updated_at_min}")
            if updated_at_max:
                query_filter_used.append(f"updated_at:<={updated_at_max}")
        
        logging.info(f"Constructed BigCommerce Query Filter: '{' AND '.join(query_filter_used)}'")
        
        # Fetch data using existing v2 fetcher
        if order_id:
            # Single order fetch
            headers = {
                'X-Auth-Token': auth_token,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            url = f"{base_url}{endpoint}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                order_data = response.json()
                all_orders = [order_data] if order_data else []
                total_pages_checked = 1
            else:
                logging.error(f"API request failed with status {response.status_code}")
                return {
                    "error": "API_CALL_FAILED",
                    "status_code": response.status_code,
                    "response_text": response.text
                }
        else:
            # Multiple orders fetch
            all_orders = fetch_v2_data(auth_token, base_url, endpoint, page_size, created_at_min, created_at_max)
            if isinstance(all_orders, dict) and 'error' in all_orders:
                return all_orders
            total_pages_checked = "multiple"
        
        # Enhance each order with line items and shipments status
        enhanced_orders = []
        headers = {
            'X-Auth-Token': auth_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        for order in all_orders:
            order_id = order.get('id')
            if not order_id:
                enhanced_orders.append(order)
                continue
                
            enhanced_order = order.copy()
            
            try:
                # Fetch line items with status information
                line_items_url = f"{base_url}/v2/orders/{order_id}/products"
                line_response = requests.get(line_items_url, headers=headers, timeout=30)
                if line_response.status_code == 200:
                    line_items = line_response.json()
                    # Just include the raw line items with all quantity columns for SQL analysis
                    enhanced_order['line_items_with_status'] = line_items
                else:
                    logging.warning(f"Failed to fetch line items for order {order_id}: {line_response.status_code}")
                    enhanced_order['line_items_with_status'] = []
                
                # Fetch shipments with status information
                shipments_url = f"{base_url}/v2/orders/{order_id}/shipments"
                shipment_response = requests.get(shipments_url, headers=headers, timeout=30)
                if shipment_response.status_code == 200:
                    shipments = shipment_response.json()
                    # Just include the raw shipments with all tracking/date columns for SQL analysis
                    enhanced_order['shipments_with_status'] = shipments
                else:
                    logging.warning(f"Failed to fetch shipments for order {order_id}: {shipment_response.status_code}")
                    enhanced_order['shipments_with_status'] = []
                    
            except Exception as e:
                logging.error(f"Error enhancing order {order_id}: {str(e)}")
                enhanced_order['line_items_with_status'] = []
                enhanced_order['shipments_with_status'] = []
            
            enhanced_orders.append(enhanced_order)
        
        logging.info(f"Completed BigCommerce status fetch with line items and shipments. Total orders: {len(enhanced_orders)}")
        
        return {
            "data": enhanced_orders,
            "total_count": len(enhanced_orders),
            "query_filter_used": ' AND '.join(query_filter_used) if query_filter_used else 'No filters',
            "total_pages_checked": total_pages_checked
        }
        
    except Exception as e:
        logging.error(f"Error fetching BigCommerce statuses: {str(e)}")
        import traceback
        return {"error": "FETCH_ERROR", "message": str(e), "traceback": traceback.format_exc()}


def _transform_bigcommerce_order(raw_order: dict) -> dict:
    """
    Transform BigCommerce order data (equivalent to Shopify's _transform_order)
    Flattens nested data and adds status fields at each level
    """
    if not raw_order:
        return None
        
    try:
        # Start with the raw order data
        transformed_order = raw_order.copy()
        
        # Add line items with only essential fields
        if 'line_items_with_status' in transformed_order:
            line_items = transformed_order['line_items_with_status']
            # Keep only essential line item fields
            minimal_line_items = []
            for item in line_items:
                minimal_item = {
                    'id': item.get('id'),
                    'order_id': item.get('order_id'),
                    'product_id': item.get('product_id'),
                    'name': item.get('name'),
                    'quantity': item.get('quantity'),
                    'quantity_shipped': item.get('quantity_shipped'),
                    'quantity_refunded': item.get('quantity_refunded'),
                    'is_refunded': item.get('is_refunded')
                }
                minimal_line_items.append(minimal_item)
            transformed_order['lineItems'] = minimal_line_items
            del transformed_order['line_items_with_status']
        
        # Add shipments with only essential fields
        if 'shipments_with_status' in transformed_order:
            shipments = transformed_order['shipments_with_status']
            # Keep only essential shipment fields
            minimal_shipments = []
            for shipment in shipments:
                minimal_shipment = {
                    'id': shipment.get('id'),
                    'order_id': shipment.get('order_id'),
                    'date_created': shipment.get('date_created')
                }
                minimal_shipments.append(minimal_shipment)
            transformed_order['fulfillments'] = minimal_shipments
            del transformed_order['shipments_with_status']
        
        # Keep only essential order fields
        essential_order = {
            'id': transformed_order.get('id'),
            'status': transformed_order.get('status'),
            'status_id': transformed_order.get('status_id'),
            'custom_status': transformed_order.get('custom_status'),
            'is_deleted': transformed_order.get('is_deleted'),
            'payment_status': transformed_order.get('payment_status'),
            'items_total': transformed_order.get('items_total'),
            'items_shipped': transformed_order.get('items_shipped'),
            'date_created': transformed_order.get('date_created'),
            'date_modified': transformed_order.get('date_modified'),
            'date_shipped': transformed_order.get('date_shipped')
        }
        
        # Add line items and fulfillments to essential order
        if 'lineItems' in transformed_order:
            essential_order['lineItems'] = transformed_order['lineItems']
        if 'fulfillments' in transformed_order:
            essential_order['fulfillments'] = transformed_order['fulfillments']
        
        # Ensure we have an ID for filename generation
        if not essential_order.get('id'):
            logging.warning("Order missing ID field")
            return None
            
        return essential_order
        
    except Exception as e:
        logging.error(f"Error transforming BigCommerce order: {str(e)}")
        return None


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


def analyze_line_item_status(line_item: dict) -> dict:
    """
    Analyze line item status and fulfillment information.
    """
    quantity = line_item.get('quantity', 0)
    quantity_shipped = line_item.get('quantity_shipped', 0)
    quantity_refunded = line_item.get('quantity_refunded', 0)
    is_refunded = line_item.get('is_refunded', False)
    
    # Calculate fulfillment status for this line item
    if quantity == 0:
        fulfillment_status = 'no_quantity'
        fulfillment_percentage = 0
    elif quantity_shipped >= quantity:
        fulfillment_status = 'fully_shipped'
        fulfillment_percentage = 100
    elif quantity_shipped > 0:
        fulfillment_status = 'partially_shipped'
        fulfillment_percentage = round((quantity_shipped / quantity) * 100, 2)
    else:
        fulfillment_status = 'pending_shipment'
        fulfillment_percentage = 0
    
    # Calculate refund status
    refund_percentage = round((quantity_refunded / quantity) * 100, 2) if quantity > 0 else 0
    
    return {
        'fulfillment_status': fulfillment_status,
        'fulfillment_percentage': fulfillment_percentage,
        'quantity_ordered': quantity,
        'quantity_shipped': quantity_shipped,
        'quantity_pending': max(0, quantity - quantity_shipped),
        'refund_status': {
            'is_refunded': is_refunded,
            'quantity_refunded': quantity_refunded,
            'refund_percentage': refund_percentage
        },
        'line_item_complete': quantity_shipped >= quantity,
        'requires_attention': quantity_shipped == 0 and quantity > 0
    }


def analyze_shipment_status(shipment: dict) -> dict:
    """
    Analyze shipment status and tracking information.
    """
    tracking_number = shipment.get('tracking_number', '')
    shipping_method = shipment.get('shipping_method', '')
    date_created = shipment.get('date_created')
    items = shipment.get('items', [])
    
    # Calculate shipment completeness
    total_items_in_shipment = len(items)
    
    # Tracking analysis
    has_tracking = bool(tracking_number)
    tracking_carrier = shipment.get('shipping_provider', 'Unknown')
    
    # Calculate shipment age
    shipment_age_hours = None
    if date_created:
        try:
            from datetime import datetime, timezone
            import dateutil.parser
            created_dt = dateutil.parser.parse(date_created)
            now = datetime.now(timezone.utc)
            shipment_age_hours = round((now - created_dt).total_seconds() / 3600, 2)
        except Exception as e:
            logging.warning(f"Error calculating shipment age: {e}")
    
    # Determine shipment status
    if has_tracking:
        shipment_status = 'shipped_with_tracking'
    elif date_created:
        shipment_status = 'shipped_no_tracking'
    else:
        shipment_status = 'preparing'
    
    return {
        'shipment_status': shipment_status,
        'tracking_info': {
            'has_tracking': has_tracking,
            'tracking_number': tracking_number,
            'carrier': tracking_carrier,
            'shipping_method': shipping_method
        },
        'shipment_details': {
            'items_count': total_items_in_shipment,
            'date_created': date_created,
            'age_hours': shipment_age_hours
        },
        'delivery_status': {
            'is_trackable': has_tracking,
            'estimated_delivery': None  # Could be enhanced with carrier-specific logic
        }
    }


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
