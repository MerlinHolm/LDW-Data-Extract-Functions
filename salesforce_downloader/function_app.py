import azure.functions as func
import json
import logging
import requests
import time
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
import base64

app = func.FunctionApp()

@app.route(route="get_product_data", auth_level=func.AuthLevel.FUNCTION)
def get_product_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Salesforce Commerce Cloud product data request')
    
    try:
        # Get required parameters
        client_id = req.params.get('client_id')
        client_secret = req.params.get('client_secret')
        datalake_key = req.params.get('datalake_key')
        
        if not client_id:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: client_id"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not client_secret:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: client_secret"}),
                status_code=400,
                mimetype="application/json"
            )
            
        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: datalake_key"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get other parameters with defaults (updated for new SFCC configuration)
        short_code = req.params.get('short_code', 'zxvetsfd')
        realm_id = req.params.get('realm_id', 'aaue')
        instance_id = req.params.get('instance_id', 'prd')
        
        # Build base_url from short_code or use provided base_url
        base_url = req.params.get('base_url')
        if not base_url:
            base_url = f'https://{short_code}.api.commercecloud.salesforce.com'
        elif not base_url.startswith('http://') and not base_url.startswith('https://'):
            base_url = f'https://{base_url}'
            
        api_version = req.params.get('api_version', 'v1')
        
        # Build organization_id from realm_id and instance_id or use provided
        organization_id = req.params.get('organization_id')
        if not organization_id:
            organization_id = f'f_ecom_{realm_id}_{instance_id}'
            
        site_id = req.params.get('site_id', 'samsonitecostco')
        data_lake_path = req.params.get('data_lake_path')
        filename = req.params.get('filename')
        page_size = req.params.get('page_size', '200')
        catalog_id = req.params.get('catalog_id')

        if not data_lake_path:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: data_lake_path"}),
                status_code=400,
                mimetype="application/json"
            )

        if not filename:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: filename"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get OAuth token
        access_token = get_salesforce_access_token(client_id, client_secret, realm_id, instance_id)
        if not access_token:
            return func.HttpResponse(
                json.dumps({"error": "Failed to obtain access token", "debug": "Check OAuth2 credentials and endpoint"}),
                status_code=401,
                mimetype="application/json"
            )
        
        # Fetch combined product data (products + inventory + pricing)
        product_data = fetch_salesforce_products(access_token, base_url, organization_id, site_id, page_size, catalog_id)
        
        # Check for errors
        items_list = product_data.get('data', [])
        has_items = len(items_list) > 0
        has_errors = 'error' in product_data

        if has_errors:
            return func.HttpResponse(
                json.dumps(product_data),
                status_code=500,
                mimetype="application/json"
            )

        # Construct the final filename by appending '-products'
        final_filename = f"{filename}-products"

        save_result = save_to_datalake(product_data, datalake_key, data_lake_path, final_filename)

        if save_result:
            # The save_to_datalake function adds .json, so reflect that in the response
            response_filename = f"{final_filename}.json"
            response_data = {
                "status": "success",
                "message": "Successfully downloaded and saved combined product data",
                "records_count": len(items_list),
                "filename": response_filename,
                "path": data_lake_path
            }
        else:
            response_data = {
                "status": "error",
                "message": "Data retrieved but failed to save to Data Lake",
                "records_count": len(items_list),
                "filename": None,
                "path": None
            }

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Unexpected error in get_product_data: {str(e)}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        
        error_response = {
            "error": "UNEXPECTED_ERROR",
            "message": f"An unexpected error occurred: {str(e)}",
            "traceback": traceback.format_exc()
        }
        
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="get_refund_data", auth_level=func.AuthLevel.FUNCTION)
def get_refund_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Salesforce Commerce Cloud refund data request')
    
    try:
        client_id = req.params.get('client_id')
        client_secret = req.params.get('client_secret')
        datalake_key = req.params.get('datalake_key')
        
        if not all([client_id, client_secret, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "Missing one or more required parameters: client_id, client_secret, datalake_key"}),
                status_code=400, mimetype="application/json"
            )
            
        base_url = req.params.get('base_url', 'kv7kzm78.api.commercecloud.salesforce.com')
        if not base_url.startswith('http'):
            base_url = f'https://{base_url}'
        api_version = req.params.get('api_version', 'v1')
        organization_id = req.params.get('organization_id', 'f_ecom_zysr_001')
        site_id = req.params.get('site_id', 'RefArchUS')
        limit = req.params.get('limit', '200')
        data_lake_path = req.params.get('data_lake_path', 'RetailOrders/input/files/json/refunds')
        filename_prefix = req.params.get('filename', 'refunds')
        start_date = req.params.get('start_date')
        end_date = req.params.get('end_date')
        
        access_token = get_salesforce_access_token(client_id, client_secret)
        if not access_token:
            return func.HttpResponse(
                json.dumps({"error": "Failed to obtain access token"}),
                status_code=401, mimetype="application/json"
            )
        
        # Use the existing orders function which expands payment details
        order_data = fetch_salesforce_orders(
            access_token, base_url, api_version, organization_id, 
            site_id, limit, start_date, end_date
        )
        
        if 'error' in order_data:
            return func.HttpResponse(
                json.dumps(order_data), status_code=500, mimetype="application/json"
            )
        
        # The refund data is within the orders, so we treat orders as the source
        refund_list = order_data.get('data', [])
        if not refund_list:
            return func.HttpResponse(
                json.dumps({"status": "success", "message": "No orders found, so no refund data available"}),
                status_code=200, mimetype="application/json"
            )
        
        date_for_filename = (start_date.replace('-', '') if start_date else datetime.now().strftime("%Y%m%d"))
        filename = f"{filename_prefix}.{date_for_filename}-refunds"
        
        save_result = save_to_datalake(order_data, datalake_key, data_lake_path, filename)
        
        if save_result:
            response_data = {
                "status": "success",
                "message": "Successfully downloaded order data containing refund details and saved to Data Lake",
                "records_count": len(refund_list),
                "filename": f"{filename}.json",
                "path": data_lake_path
            }
            return func.HttpResponse(json.dumps(response_data), status_code=200, mimetype="application/json")
        else:
            return func.HttpResponse(
                json.dumps({"error": "Failed to save data to Data Lake"}),
                status_code=500, mimetype="application/json"
            )
            
    except Exception as e:
        logging.error(f"Error in get_refund_data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500, mimetype="application/json"
        )


@app.route(route="get_order_data", auth_level=func.AuthLevel.FUNCTION)
def get_order_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Salesforce Commerce Cloud order data request')
    
    try:
        # Get required parameters
        client_id = req.params.get('client_id')
        client_secret = req.params.get('client_secret')
        datalake_key = req.params.get('datalake_key')
        
        if not client_id:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: client_id"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not client_secret:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: client_secret"}),
                status_code=400,
                mimetype="application/json"
            )
            
        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: datalake_key"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get other parameters with defaults (updated for new SFCC configuration)
        short_code = req.params.get('short_code', 'zxvetsfd')
        realm_id = req.params.get('realm_id', 'aaue')
        instance_id = req.params.get('instance_id', 'prd')
        
        # Build base_url from short_code or use provided base_url
        base_url = req.params.get('base_url')
        if not base_url:
            base_url = f'https://{short_code}.api.commercecloud.salesforce.com'
        elif not base_url.startswith('http://') and not base_url.startswith('https://'):
            base_url = f'https://{base_url}'
            
        api_version = req.params.get('api_version', 'v1')
        
        # Build organization_id from realm_id and instance_id or use provided
        organization_id = req.params.get('organization_id')
        if not organization_id:
            organization_id = f'f_ecom_{realm_id}_{instance_id}'
            
        site_id = req.params.get('site_id', 'samsonitecostco')
        limit = req.params.get('limit', '200')
        data_lake_path = req.params.get('data_lake_path', 'RetailOrders/input/files/json/orders')
        filename_prefix = req.params.get('filename', 'orders')
        start_date = req.params.get('start_date')
        end_date = req.params.get('end_date')
        
        # Step 1: Get OAuth2 access token
        access_token = get_salesforce_access_token(client_id, client_secret, realm_id, instance_id)
        if not access_token:
            return func.HttpResponse(
                json.dumps({"error": "Failed to obtain access token", "debug": "Check OAuth2 credentials and endpoint"}),
                status_code=401,
                mimetype="application/json"
            )
        
        # Step 2: Fetch order data from Salesforce Commerce Cloud
        orders_data = fetch_salesforce_orders(
            access_token, 
            base_url,
            api_version,
            organization_id, 
            site_id, 
            limit,
            start_date,
            end_date
        )
        
        if not orders_data:
            # Construct the full URL that would be called for debugging
            debug_url = f"{base_url}/checkout/orders/{api_version}/organizations/{organization_id}/orders?siteId={site_id}&exportStatus=exported&limit={limit}"
            if start_date and end_date:
                debug_url += f"&creationDateFrom={start_date}&creationDateTo={end_date}"
            
            return func.HttpResponse(
                json.dumps({
                    "error": "Failed to fetch order data", 
                    "debug": {
                        "access_token_obtained": bool(access_token),
                        "access_token_length": len(access_token) if access_token else 0,
                        "api_url": f"{base_url}/checkout/orders/{api_version}/organizations/{organization_id}/orders",
                        "full_url_with_params": debug_url,
                        "parameters": {
                            "siteId": site_id,
                            "exportStatus": "exported",
                            "limit": limit,
                            "creationDateFrom": start_date,
                            "creationDateTo": end_date
                        },
                        "working_url": "https://kv7kzm78.api.commercecloud.salesforce.com/checkout/orders/v1/organizations/f_ecom_zysr_001/orders?siteId=RefArchUS&limit=200&creationDateFrom=2025-03-22&creationDateTo=2025-08-13"
                    }
                }),
                status_code=500,
                mimetype="application/json"
            )
        
        # Step 3: Only save to Data Lake if we have actual orders (no errors, no zero records)
        orders_list = orders_data.get('data', [])
        has_orders = len(orders_list) > 0
        has_errors = 'error' in orders_data
        
        if has_errors:
            # Return error without saving any file
            return func.HttpResponse(
                json.dumps(orders_data),
                status_code=500,
                mimetype="application/json"
            )
        
        if not has_orders:
            # Return success but don't save file when no orders found
            response_data = {
                "status": "success",
                "message": "No orders found in the specified date range",
                "records_count": 0,
                "filename": None,
                "path": None,
                "note": "No file created - no orders to save"
            }
            return func.HttpResponse(
                json.dumps(response_data),
                status_code=200,
                mimetype="application/json"
            )
        
        # Only save if we have actual orders
        # Use start_date if provided, otherwise current date (matching Magento format)
        if start_date:
            date_for_filename = start_date.replace('-', '')  # Convert YYYY-MM-DD to YYYYMMDD
        else:
            date_for_filename = datetime.now().strftime("%Y%m%d")
        filename = f"{filename_prefix}.{date_for_filename}-orders"
        
        save_result = save_to_datalake(orders_data, datalake_key, data_lake_path, filename)
        
        if save_result:
            response_data = {
                "status": "success",
                "message": f"Successfully downloaded and saved order data",
                "records_count": len(orders_data.get('data', [])),
                "filename": filename,
                "path": data_lake_path
            }
            return func.HttpResponse(
                json.dumps(response_data),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({
                    "error": "Failed to save data to Data Lake",
                    "debug": {
                        "orders_fetched": len(orders_data.get('data', [])) if orders_data else 0,
                        "data_lake_path": data_lake_path,
                        "filename": filename,
                        "data_size_kb": len(str(orders_data)) // 1024 if orders_data else 0
                    }
                }),
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


def get_salesforce_access_token(client_id: str, client_secret: str, realm_id: str = 'aaue', instance_id: str = 'prd') -> str:
    """
    Get OAuth2 access token from Salesforce Commerce Cloud
    """
    try:
        # OAuth2 endpoint
        token_url = "https://account.demandware.com/dwsso/oauth2/access_token"
        
        # Prepare the request
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()}'
        }
        
        # Build scope using provided realm_id and instance_id
        # Include both orders and products scopes for comprehensive access
        data = {
            'grant_type': 'client_credentials',
            'scope': f'SALESFORCE_COMMERCE_API:{realm_id}_{instance_id} sfcc.orders sfcc.products'
        }
        
        logging.info(f"Requesting access token from Salesforce. URL: {token_url}")
        logging.info(f"Headers: {headers}")
        logging.info(f"Data: {data}")
        
        response = requests.post(token_url, headers=headers, data=data)
        
        logging.info(f"OAuth2 Response Status: {response.status_code}")
        logging.info(f"OAuth2 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            logging.info(f"Successfully obtained access token. Token length: {len(access_token) if access_token else 0}")
            return access_token
        else:
            logging.error(f"Failed to get access token. Status: {response.status_code}, Response: {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"Error getting access token: {str(e)}")
        return None



def fetch_salesforce_orders(access_token: str, base_url: str, api_version: str, organization_id: str, site_id: str, limit: str, start_date: str = None, end_date: str = None) -> dict:
    """
    Fetch orders from Salesforce Commerce Cloud using the new SFCC API endpoints
    """
    try:
        # Build the API URL using the new SFCC format
        api_path = f"/checkout/orders/{api_version}"
        url = f"{base_url}{api_path}/organizations/{organization_id}/orders"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Prepare query parameters using the new SFCC format
        params = {
            'siteId': site_id,
            'limit': limit,
            'exportStatus': 'exported'  # Use exportStatus filter as shown in the working query
        }

        # Add date filters if provided (keeping original parameter names for compatibility)
        if start_date and end_date:
            # SFCC might need full datetime format, try both formats
            if 'T' not in start_date:
                # Add time component if not present
                start_date_formatted = f"{start_date}T00:00:00.000Z"
                end_date_formatted = f"{end_date}T23:59:59.999Z"
            else:
                start_date_formatted = start_date
                end_date_formatted = end_date
                
            params['creationDateFrom'] = start_date_formatted
            params['creationDateTo'] = end_date_formatted
            
            logging.info(f"Date filtering: Original start={start_date}, end={end_date}")
            logging.info(f"Date filtering: Formatted start={start_date_formatted}, end={end_date_formatted}")
        elif start_date:
            # If only start_date provided, add time component
            if 'T' not in start_date:
                start_date_formatted = f"{start_date}T00:00:00.000Z"
            else:
                start_date_formatted = start_date
            params['creationDateFrom'] = start_date_formatted
            logging.info(f"Date filtering: Start date only - {start_date_formatted}")
        elif end_date:
            # If only end_date provided, add time component
            if 'T' not in end_date:
                end_date_formatted = f"{end_date}T23:59:59.999Z"
            else:
                end_date_formatted = end_date
            params['creationDateTo'] = end_date_formatted
            logging.info(f"Date filtering: End date only - {end_date_formatted}")
        
        logging.info(f"Fetching orders from Salesforce Commerce Cloud: {url}")
        logging.info(f"API Headers: {headers}")
        logging.info(f"API Params: {params}")
        
        all_orders = []
        offset = 0
        max_pages = 100  # Increased safety limit to handle larger datasets
        page_count = 0
        
        while page_count < max_pages:
            # Add offset for pagination
            current_params = params.copy()
            current_params['offset'] = offset
            page_count += 1
            
            logging.info(f"Making API call - Page {page_count}, Offset: {offset}")
            logging.info(f"Full URL: {url}")
            logging.info(f"Parameters: {current_params}")
            
            response = requests.get(url, headers=headers, params=current_params, timeout=30)
            
            logging.info(f"API Response Status: {response.status_code}")
            logging.info(f"API Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Handle different response structures from SFCC API
                orders = []
                if isinstance(data, dict):
                    # Check for different possible data keys
                    orders = data.get('data', data.get('orders', data.get('hits', [])))
                elif isinstance(data, list):
                    orders = data
                
                logging.info(f"API Response Data Keys: {list(data.keys()) if isinstance(data, dict) else 'List response'}")
                logging.info(f"Orders found in response: {len(orders)}")
                
                if not orders:
                    logging.info("No orders found in response, breaking pagination loop")
                    
                    # If this is the first page and we're using date filters, log debug info
                    if page_count == 1 and (start_date or end_date):
                        logging.warning(f"No orders found with date filters. Date range: {params.get('creationDateFrom', 'None')} to {params.get('creationDateTo', 'None')}")
                        logging.warning("Consider:")
                        logging.warning("1. Checking if the order creation date is within the specified range")
                        logging.warning("2. Trying a broader date range")
                        logging.warning("3. Checking if the order exists without date filters")
                        logging.warning("4. Verifying the time zone - SFCC might use UTC")
                    
                    break
                
                # Transform each order using the transform function directly
                enhanced_orders = []
                for order in orders:
                    try:
                        # Get order ID for transformation
                        order_id = order.get('orderNo') or order.get('id') or order.get('orderNumber')
                        if order_id:
                            logging.info(f"Transforming order {order_id} from list data")
                            
                            # Try to get individual order details first for better data
                            detailed_order = fetch_individual_order(access_token, base_url, api_version, organization_id, site_id, order_id)
                            if detailed_order:
                                logging.info(f"✅ Got detailed order data for {order_id}, using that for transformation")
                                # Also try to fetch shipments separately if not included
                                shipments = fetch_order_shipments(access_token, base_url, api_version, organization_id, site_id, order_id)
                                if shipments:
                                    detailed_order['additional_shipments'] = shipments
                                enhanced_orders.append(detailed_order)
                            else:
                                logging.warning(f"⚠️ Individual order fetch failed for {order_id}, transforming list data instead")
                                # Transform the list order data directly
                                transformed_order = transform_sfcc_order_data(order, order_id)
                                enhanced_orders.append(transformed_order)
                        else:
                            logging.warning(f"⚠️ No order ID found in order data, using raw order")
                            enhanced_orders.append(order)
                    except Exception as e:
                        logging.error(f"❌ Failed to process order {order.get('orderNo', 'unknown')}: {str(e)}")
                        # As last resort, try to transform the raw order
                        try:
                            order_id = order.get('orderNo') or order.get('id') or order.get('orderNumber') or 'unknown'
                            logging.info(f"🔄 Attempting emergency transform for order {order_id}")
                            transformed_order = transform_sfcc_order_data(order, order_id)
                            enhanced_orders.append(transformed_order)
                        except Exception as transform_error:
                            logging.error(f"❌ Emergency transform also failed for {order_id}: {str(transform_error)}")
                            enhanced_orders.append(order)  # Use original as absolute last resort
                
                all_orders.extend(enhanced_orders)
                logging.info(f"Fetched {len(enhanced_orders)} orders (total: {len(all_orders)})")
                
                # Check API response for pagination info
                total_count = data.get('total', data.get('count', None))
                if total_count:
                    logging.info(f"API reports total available: {total_count}")
                    if len(all_orders) >= total_count:
                        logging.info(f"Fetched all available orders: {len(all_orders)}/{total_count}")
                        break
                
                # Check if there are more pages (standard pagination check)
                if len(orders) < int(limit):
                    logging.info(f"Received {len(orders)} orders, less than limit {limit}. Ending pagination.")
                    break
                
                # Check for next page indicators
                has_more = data.get('hasMore', data.get('has_more', True))
                if not has_more:
                    logging.info("API indicates no more pages available")
                    break
                    
                offset += int(limit)
                logging.info(f"Continuing to next page. New offset: {offset}")
                
            else:
                logging.error(f"Failed to fetch orders. Status: {response.status_code}")
                logging.error(f"Response Headers: {dict(response.headers)}")
                logging.error(f"Response Text: {response.text}")
                # Return detailed error info for debugging
                return {
                    "error": "API_CALL_FAILED",
                    "status_code": response.status_code,
                    "response_text": response.text,
                    "response_headers": dict(response.headers),
                    "request_url": url,
                    "request_params": current_params,
                    "debug_info": {
                        "expected_url_format": f"{base_url}/checkout/orders/v1/organizations/{organization_id}/orders?siteId={site_id}&exportStatus=exported&limit=200",
                        "working_example": "https://zxvetsfd.api.commercecloud.salesforce.com/checkout/orders/v1/organizations/f_ecom_aaue_prd/orders?siteId=samsonitecostco&exportStatus=exported&limit=200"
                    }
                }
        
        result = {
            'data': all_orders,
            'total_count': len(all_orders),
            'fetch_timestamp': datetime.now().isoformat(),
            'metadata': {
                'api_endpoint': url,
                'organization_id': organization_id,
                'site_id': site_id,
                'pages_fetched': page_count,
                'parameters_used': params
            }
        }
        
        if page_count >= max_pages:
            logging.warning(f"Reached maximum page limit ({max_pages}). May have more data available.")
            logging.warning(f"Consider increasing max_pages or using date filters to reduce dataset size.")
        
        logging.info(f"Successfully fetched {len(all_orders)} orders from Salesforce in {page_count} pages")
        logging.info(f"Final pagination stats: Pages={page_count}, Max Pages={max_pages}, Orders per page avg={len(all_orders)/page_count if page_count > 0 else 0:.1f}")
        return result
        
    except Exception as e:
        logging.error(f"Error fetching Salesforce orders: {str(e)}")
        return None


def fetch_individual_order(access_token: str, base_url: str, api_version: str, organization_id: str, site_id: str, order_id: str) -> dict:
    """
    Fetch comprehensive order details from Salesforce Commerce Cloud
    Includes: Order, Line Items, Shipments, Shipment Lines, Returns, Return Lines
    """
    try:
        # Build the individual order URL with expand parameters for comprehensive data
        api_path = f"/checkout/orders/{api_version}"
        url = f"{base_url}{api_path}/organizations/{organization_id}/orders/{order_id}"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Prepare query parameters with expand to get all related data
        # Try different expand combinations to ensure we get all line items
        params = {
            'siteId': site_id,
            'expand': 'productItems,payments,paymentInstruments,shipments,notes,productLineItems'  # Request all related data including alternative line item names
        }
        
        logging.info(f"Fetching comprehensive order data for {order_id} from: {url}")
        logging.info(f"Expand parameters: {params['expand']}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            order_data = response.json()
            
            # Debug logging to understand the API response structure
            logging.info(f"Raw order data keys for {order_id}: {list(order_data.keys()) if isinstance(order_data, dict) else 'Not a dict'}")
            if isinstance(order_data, dict):
                product_items = order_data.get('productItems', [])
                logging.info(f"Raw productItems count for {order_id}: {len(product_items)}")
                if product_items:
                    logging.info(f"First productItem keys: {list(product_items[0].keys()) if product_items else 'No items'}")
                else:
                    logging.warning(f"No productItems found in order {order_id}. Available keys: {list(order_data.keys())}")
            
            # Transform the SFCC order data to match Shopify/BigCommerce structure
            enhanced_order = transform_sfcc_order_data(order_data, order_id)
            
            logging.info(f"Successfully fetched and transformed order {order_id}")
            logging.info(f"Order contains: {len(enhanced_order.get('lineItems', []))} line items, {len(enhanced_order.get('fulfillments', []))} shipments")
            
            return enhanced_order
        else:
            logging.warning(f"Failed to fetch individual order {order_id}. Status: {response.status_code}")
            logging.warning(f"Response: {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"Error fetching individual order {order_id}: {str(e)}")
        return None


def fetch_order_shipments(access_token: str, base_url: str, api_version: str, organization_id: str, site_id: str, order_id: str) -> list:
    """
    Fetch shipments for a specific order from Salesforce Commerce Cloud
    This is a separate API call in case shipments are not included in the order expand
    """
    try:
        # Build the shipments URL for this order
        api_path = f"/checkout/orders/{api_version}"
        url = f"{base_url}{api_path}/organizations/{organization_id}/orders/{order_id}/shipments"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Prepare query parameters
        params = {
            'siteId': site_id,
            'expand': 'productItems,productLineItems,lineItems,items'  # Request all line item variations
        }
        
        logging.info(f"Fetching shipments for order {order_id} from: {url}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            shipments_data = response.json()
            
            # Handle different response structures
            shipments = []
            if isinstance(shipments_data, dict):
                shipments = shipments_data.get('data', shipments_data.get('shipments', shipments_data.get('hits', [])))
            elif isinstance(shipments_data, list):
                shipments = shipments_data
            
            logging.info(f"Successfully fetched {len(shipments)} shipments for order {order_id}")
            return shipments
        else:
            logging.info(f"No separate shipments endpoint available for order {order_id}. Status: {response.status_code}")
            return []
            
    except Exception as e:
        logging.warning(f"Error fetching shipments for order {order_id}: {str(e)}")
        return []


def transform_sfcc_order_data(sfcc_order: dict, order_id: str) -> dict:
    """
    Transform SFCC order data to match Shopify/BigCommerce structure
    Creates: Orders, Line Items, Shipments, Shipment Lines, Returns, Return Lines
    """
    try:
        # Start with the base order data
        transformed_order = sfcc_order.copy()
        
        # Extract and transform line items (product items)
        line_items = []
        
        # Try different possible field names for line items in SFCC
        product_items = sfcc_order.get('productItems', [])
        if not product_items:
            product_items = sfcc_order.get('productLineItems', [])
        if not product_items:
            product_items = sfcc_order.get('lineItems', [])
        if not product_items:
            product_items = sfcc_order.get('items', [])
        
        logging.info(f"Transform function - Order {order_id}: Found {len(product_items)} line items in raw data")
        if product_items:
            logging.info(f"Transform function - First line item structure: {list(product_items[0].keys())}")
        else:
            logging.warning(f"Transform function - No line items found for order {order_id}. Available top-level keys: {list(sfcc_order.keys())}")
        
        for idx, item in enumerate(product_items):
            # Debug logging for order line item structure
            logging.info(f"Order line item {idx} keys: {list(item.keys())}")
            logging.info(f"Order line item {idx} data: itemId={item.get('itemId')}, productId={item.get('productId')}, quantity={item.get('quantity')}")
            
            # Try multiple possible ID fields for order line items
            order_line_id = item.get('itemId') or item.get('lineItemId') or item.get('productLineItemId') or item.get('id') or f"{order_id}_line_{idx}"
            
            # Get variant ID and derive master product ID using same logic as product data
            variant_id = item.get('productId')
            
            # Extract master product ID from variant ID using same pattern matching as product data
            if variant_id:
                # Check if this looks like a variant ID (numeric ending)
                if variant_id[-4:].isdigit() or any(c.isalpha() for c in variant_id[-4:]):
                    # Find the base pattern (remove the variant-specific ending)
                    master_base = variant_id
                    # Remove common variant endings (4 chars) and add XXXX
                    if len(variant_id) > 4:
                        master_base = variant_id[:-4] + 'XXXX'
                    master_product_id = master_base
                else:
                    # If it doesn't look like a variant, it might already be a master
                    master_product_id = variant_id
            else:
                # Fallback if no productId
                master_product_id = item.get('masterProductId', variant_id)
            
            # Debug logging for ID mapping
            logging.info(f"Order line item ID mapping: variant_id={variant_id} -> master_product_id={master_product_id}")
            
            line_item = {
                # Standard line item fields
                'id': order_line_id,
                'order_id': order_id,
                'product_id': master_product_id,      # Master product ID
                'variant_id': variant_id,             # Specific variant ID
                'sku': variant_id,                    # SKU is the variant ID
                'name': item.get('productName', ''),
                'quantity': item.get('quantity', 0),
                'price': item.get('basePrice', item.get('netPrice', item.get('price', 0))),
                'shipment_id': item.get('shipmentId', ''),  # This is the key for joining!
                
                # ALL SFCC productItems fields - include everything from raw data
                'itemId': item.get('itemId', ''),
                'productId': item.get('productId', ''),
                'productName': item.get('productName', ''),
                'itemText': item.get('itemText', ''),
                'basePrice': item.get('basePrice', 0),
                'netPrice': item.get('netPrice', 0),
                'grossPrice': item.get('grossPrice', 0),
                'priceAfterItemDiscount': item.get('priceAfterItemDiscount', 0),
                'priceAfterOrderDiscount': item.get('priceAfterOrderDiscount', 0),
                'adjustedTax': item.get('adjustedTax', 0),
                'tax': item.get('tax', 0),
                'taxBasis': item.get('taxBasis', 0),
                'taxRate': item.get('taxRate', 0),
                'brand': item.get('brand', ''),
                'gift': item.get('gift', False),
                'giftMessage': item.get('giftMessage', ''),
                'bonusProductLineItem': item.get('bonusProductLineItem', False),
                'bundledProductLineItem': item.get('bundledProductLineItem', False),
                'optionProductLineItem': item.get('optionProductLineItem', False),
                'productListItem': item.get('productListItem', False),
                'minOrderQuantity': item.get('minOrderQuantity', 1),
                'stepQuantity': item.get('stepQuantity', 1),
                'position': item.get('position', 0),
                'inventoryId': item.get('inventoryId', ''),
                
                # Add fulfillment status tracking
                'fulfillment_status': 'fulfilled' if item.get('c_orderItemShippedQuantity', 0) > 0 else 'unfulfilled',
                'quantity_fulfilled': item.get('c_orderItemShippedQuantity', 0),
                'quantity_shipped': item.get('c_orderItemShippedQuantity', 0),
                'quantity_returned': item.get('c_orderItemReturnedQuantity', 0),
                
                # Include ALL other productItem fields dynamically
                **{k: v for k, v in item.items() if k not in [
                    'id', 'order_id', 'product_id', 'variant_id', 'sku', 'name', 
                    'quantity', 'price', 'shipment_id', 'fulfillment_status',
                    'quantity_fulfilled', 'quantity_shipped', 'quantity_returned'
                ]},
                
                # Keep raw data for debugging and additional processing
                'raw_line_item_data': item
            }
            
            # Add any custom attributes
            if 'c_customAttributes' in item:
                line_item['custom_attributes'] = item['c_customAttributes']
                
            line_items.append(line_item)
        
        # Extract and transform shipments (fulfillments)
        fulfillments = []
        
        # Try different possible field names for shipments in SFCC
        shipments = sfcc_order.get('shipments', [])
        if not shipments:
            shipments = sfcc_order.get('fulfillments', [])
        if not shipments:
            shipments = sfcc_order.get('deliveries', [])
        
        logging.info(f"Transform function - Order {order_id}: Found {len(shipments)} shipments in raw data")
        if shipments:
            logging.info(f"Transform function - First shipment structure: {list(shipments[0].keys())}")
        else:
            logging.warning(f"Transform function - No shipments found for order {order_id}")
        
        for shipment_index, shipment in enumerate(shipments):
            shipment_id = shipment.get('shipmentId', '')
            shipment_no = shipment.get('shipmentNo', '')
            
            # Fix fulfillment ID - create unique IDs for multiple shipments
            if shipment_id == 'me' or not shipment_id:
                if shipment_no:
                    # Use shipmentNo directly, add index for uniqueness in case of duplicates
                    fulfillment_id = shipment_no
                    if len(shipments) > 1:
                        fulfillment_id = f"{shipment_no}_{shipment_index + 1}"
                else:
                    # Generate unique ID based on order and shipment index
                    fulfillment_id = f"fulfillment_{order_id}_{shipment_index + 1}"
            else:
                fulfillment_id = shipment_id
            
            # Debug: Log what's actually in the shipment data
            logging.info(f"🔍 Shipment {shipment_id} fields: {list(shipment.keys())}")
            logging.info(f"🔍 Shipment {shipment_id} - shippingStatus: '{shipment.get('shippingStatus', 'NOT_FOUND')}', trackingNumber: '{shipment.get('trackingNumber', 'NOT_FOUND')}'")
            logging.info(f"🔍 Shipment {shipment_id} - creationDate: '{shipment.get('creationDate', 'NOT_FOUND')}', lastModified: '{shipment.get('lastModified', 'NOT_FOUND')}')")
            logging.info(f"🔍 Fixed fulfillment ID from '{shipment_id}' to '{fulfillment_id}'")
            logging.info(f"🔍 Order {order_id} - invoiceNo: '{sfcc_order.get('invoiceNo', 'NOT_FOUND')}', shipmentNo: '{shipment_no}' (shipment {shipment_index + 1} of {len(shipments)})")
            
            # Get order-level data for better fulfillment mapping
            order_shipping_status = sfcc_order.get('shippingStatus', '')
            order_creation_date = sfcc_order.get('creationDate', '')
            order_last_modified = sfcc_order.get('lastModified', '')
            
            # Fix payment status - use cybersource status and order completion status
            payment_status = sfcc_order.get('paymentStatus', 'not_paid')
            cybersource_status = ''
            payment_instruments = sfcc_order.get('paymentInstruments', [])
            if payment_instruments:
                cybersource_status = payment_instruments[0].get('paymentTransaction', {}).get('c_cybersourceStatus', '')
            
            # Determine actual payment status
            if cybersource_status == 'AUTHORIZED' and order_shipping_status == 'shipped':
                actual_payment_status = 'paid'
            elif cybersource_status == 'AUTHORIZED':
                actual_payment_status = 'authorized'
            else:
                actual_payment_status = payment_status
            
            # Debug: Log what data we're actually getting
            logging.info(f"🔍 Order {order_id} - shippingStatus: '{order_shipping_status}', creationDate: '{order_creation_date}', lastModified: '{order_last_modified}'")
            logging.info(f"🔍 Order {order_id} - paymentStatus: '{payment_status}' -> actualPaymentStatus: '{actual_payment_status}' (cybersource: '{cybersource_status}')")
            
            # Extract tracking numbers from line items
            tracking_numbers = []
            for item in line_items:
                item_tracking = item.get('raw_line_item_data', {}).get('c_orderItemTrackingNumbers', [])
                if item_tracking:
                    tracking_numbers.extend(item_tracking)
                    logging.info(f"🔍 Found tracking numbers in line item {item.get('id', 'unknown')}: {item_tracking}")
            
            logging.info(f"🔍 Total tracking numbers found for order {order_id}: {tracking_numbers}")
            
            # Determine tracking company from tracking number format
            tracking_company = ''
            tracking_url = ''
            if tracking_numbers:
                first_tracking = tracking_numbers[0]
                if '1Z' in first_tracking:
                    tracking_company = 'UPS'
                    tracking_url = f"https://www.ups.com/track?tracknum={first_tracking}"
                elif first_tracking.isdigit() and len(first_tracking) > 10:
                    tracking_company = 'FedEx'
                    tracking_url = f"https://www.fedex.com/fedextrack/?tracknumbers={first_tracking}"
            
            fulfillment = {
                # Standard fulfillment fields - use order data when shipment data is empty
                'id': fulfillment_id,
                'order_id': order_id,
                'status': order_shipping_status or shipment.get('shippingStatus', ''),
                'tracking_company': tracking_company,
                'tracking_number': tracking_numbers[0] if tracking_numbers else shipment.get('trackingNumber', ''),
                'tracking_url': tracking_url or shipment.get('trackingUrl', ''),
                'created_at': order_creation_date or shipment.get('creationDate', ''),
                'updated_at': order_last_modified or shipment.get('lastModified', ''),
                'shipped_at': order_last_modified if order_shipping_status == 'shipped' else shipment.get('shippingDate', ''),
                'delivery_date': shipment.get('deliveryDate', ''),
                'shipping_method': shipment.get('shippingMethod', {}).get('id', ''),
                'shipping_cost': shipment.get('shippingTotal', 0),
                'shipping_tax': shipment.get('shippingTotalTax', 0),
                'gift': shipment.get('gift', False),
                'gift_message': shipment.get('giftMessage', ''),
                'shipping_address': shipment.get('shippingAddress', {}),
                'line_items': [],
                
                # Add tracking numbers array for multiple tracking numbers
                'tracking_numbers': tracking_numbers,
                
                # ALL SFCC shipment fields - include everything from raw data
                'shipmentId': shipment.get('shipmentId', ''),
                'shipmentNo': shipment.get('shipmentNo', ''),
                'shipmentTotal': shipment.get('shipmentTotal', 0),
                'adjustedMerchandizeTotalTax': shipment.get('adjustedMerchandizeTotalTax', 0),
                'adjustedShippingTotalTax': shipment.get('adjustedShippingTotalTax', 0),
                'merchandizeTotalTax': shipment.get('merchandizeTotalTax', 0),
                'productSubTotal': shipment.get('productSubTotal', 0),
                'productTotal': shipment.get('productTotal', 0),
                'shippingTotal': shipment.get('shippingTotal', 0),
                'shippingTotalTax': shipment.get('shippingTotalTax', 0),
                'taxTotal': shipment.get('taxTotal', 0),
                'shippingMethod': shipment.get('shippingMethod', {}),
                'shippingAddress': shipment.get('shippingAddress', {}),
                
                # Include ALL other shipment fields dynamically
                **{k: v for k, v in shipment.items() if k not in [
                    'id', 'order_id', 'status', 'tracking_company', 'tracking_number', 
                    'tracking_url', 'created_at', 'updated_at', 'shipped_at', 
                    'delivery_date', 'shipping_method', 'shipping_cost', 'shipping_tax',
                    'gift', 'gift_message', 'shipping_address', 'line_items'
                ]}
            }
            
            # SFCC Logic: Find order line items that belong to this shipment
            # Debug the shipment ID values
            logging.info(f"🔍 Analyzing shipment {shipment_id} (value: '{shipment_id}')")
            
            shipment_line_items = []
            for line_item in line_items:
                line_item_shipment_id = line_item.get('shipment_id')
                logging.info(f"🔍 Line item {line_item['id']} has shipmentId: '{line_item_shipment_id}'")
                
                # Check if shipmentId is a meaningful join key or just a placeholder
                if line_item_shipment_id == shipment_id:
                    # Check if this is a meaningful join or just default values
                    if shipment_id == "me" or shipment_id == "default" or not shipment_id:
                        logging.warning(f"⚠️ Shipment ID '{shipment_id}' appears to be a placeholder/default value")
                        # For default shipments, include all line items (single shipment scenario)
                        if len(shipments) == 1:
                            logging.info(f"📦 Single shipment scenario - adding all line items to shipment '{shipment_id}'")
                        else:
                            logging.warning(f"📦 Multiple shipments with placeholder ID - this may not be correct")
                    
                    # Create shipment line item from order line item - include ALL fields
                    shipment_line = {
                        # Standard shipment line fields
                        'id': line_item['id'],
                        'line_item_id': line_item['id'],
                        'item_id': line_item['id'],  # SFCC itemId for joining
                        'product_id': line_item.get('product_id', ''),
                        'quantity': line_item.get('quantity', 0),
                        'price': line_item.get('price', 0),
                        'sku': line_item.get('sku', ''),
                        'name': line_item.get('name', ''),
                        'join_method': 'shipmentId_match',
                        
                        # SFCC identifiers for joining
                        'sfcc_item_id': line_item.get('raw_line_item_data', {}).get('itemId', ''),
                        'sfcc_product_id': line_item.get('raw_line_item_data', {}).get('productId', ''),
                        'sfcc_shipment_id': line_item.get('raw_line_item_data', {}).get('shipmentId', ''),
                        
                        # ALL order line item fields - copy everything from the order line item
                        **{k: v for k, v in line_item.items() if k not in [
                            'id', 'line_item_id', 'item_id', 'product_id', 'quantity', 
                            'price', 'sku', 'name', 'join_method'
                        ]}
                    }
                    
                    # Fix fulfillment status consistency - use SFCC shipped quantity data
                    shipped_qty = line_item.get('raw_line_item_data', {}).get('c_orderItemShippedQuantity', 0)
                    if shipped_qty > 0:
                        shipment_line['fulfillment_status'] = 'fulfilled'
                        shipment_line['quantity_shipped'] = shipped_qty
                        shipment_line['quantity_fulfilled'] = shipped_qty
                    else:
                        shipment_line['fulfillment_status'] = 'unfulfilled'
                        shipment_line['quantity_shipped'] = 0
                        shipment_line['quantity_fulfilled'] = 0
                    fulfillment['line_items'].append(shipment_line)
                    shipment_line_items.append(line_item)
                    
                    # Update fulfillment status based on SFCC data
                    shipped_qty = line_item.get('raw_line_item_data', {}).get('c_orderItemShippedQuantity', 0)
                    line_item['quantity_shipped'] = shipped_qty
                    line_item['quantity_fulfilled'] = shipped_qty
                    line_item['fulfillment_status'] = 'fulfilled' if shipped_qty > 0 else 'unfulfilled'
                    
                    logging.info(f"✅ Added line item {line_item['id']} to shipment {shipment_id} via shipmentId match")
            
            # If no line items matched and this looks like a default scenario, try alternative strategies
            if len(shipment_line_items) == 0 and (shipment_id == "me" or len(shipments) == 1):
                logging.info(f"🔄 No shipmentId matches found. Trying alternative join strategies for shipment '{shipment_id}'")
                
                # Strategy: If single shipment, assign all line items to it
                if len(shipments) == 1:
                    logging.info(f"📦 Single shipment detected - assigning all {len(line_items)} line items to shipment")
                    for line_item in line_items:
                        shipment_line = {
                            # Standard shipment line fields
                            'id': line_item['id'],
                            'line_item_id': line_item['id'],
                            'item_id': line_item['id'],  # SFCC itemId for joining
                            'product_id': line_item.get('product_id', ''),
                            'quantity': line_item.get('quantity', 0),
                            'price': line_item.get('price', 0),
                            'sku': line_item.get('sku', ''),
                            'name': line_item.get('name', ''),
                            'join_method': 'single_shipment_fallback',
                            
                            # SFCC identifiers for joining
                            'sfcc_item_id': line_item.get('raw_line_item_data', {}).get('itemId', ''),
                            'sfcc_product_id': line_item.get('raw_line_item_data', {}).get('productId', ''),
                            'sfcc_shipment_id': line_item.get('raw_line_item_data', {}).get('shipmentId', ''),
                            
                            # ALL order line item fields - copy everything from the order line item
                            **{k: v for k, v in line_item.items() if k not in [
                                'id', 'line_item_id', 'item_id', 'product_id', 'quantity', 
                                'price', 'sku', 'name', 'join_method'
                            ]}
                        }
                        
                        # Fix fulfillment status consistency - use SFCC shipped quantity data
                        shipped_qty = line_item.get('raw_line_item_data', {}).get('c_orderItemShippedQuantity', 0)
                        if shipped_qty > 0:
                            shipment_line['fulfillment_status'] = 'fulfilled'
                            shipment_line['quantity_shipped'] = shipped_qty
                            shipment_line['quantity_fulfilled'] = shipped_qty
                        else:
                            shipment_line['fulfillment_status'] = 'unfulfilled'
                            shipment_line['quantity_shipped'] = 0
                            shipment_line['quantity_fulfilled'] = 0
                        fulfillment['line_items'].append(shipment_line)
                        shipment_line_items.append(line_item)
                        
                        # Update fulfillment status based on SFCC data
                        shipped_qty = line_item.get('raw_line_item_data', {}).get('c_orderItemShippedQuantity', 0)
                        line_item['quantity_shipped'] = shipped_qty
                        line_item['quantity_fulfilled'] = shipped_qty
                        line_item['fulfillment_status'] = 'fulfilled' if shipped_qty > 0 else 'unfulfilled'
                        
                        logging.info(f"✅ Added line item {line_item['id']} to shipment {shipment_id} via single-shipment fallback")
            
            logging.info(f"Transform function - Shipment {shipment_id}: Contains {len(shipment_line_items)} line items")
            
            fulfillments.append(fulfillment)
        
        # Process additional shipments if they were fetched separately
        if 'additional_shipments' in sfcc_order:
            additional_shipments = sfcc_order['additional_shipments']
            logging.info(f"Transform function - Order {order_id}: Processing {len(additional_shipments)} additional shipments")
            
            for shipment in additional_shipments:
                fulfillment = {
                    'id': shipment.get('shipmentId', ''),
                    'order_id': order_id,
                    'status': shipment.get('status', ''),
                    'tracking_company': shipment.get('trackingNumber', ''),
                    'tracking_number': shipment.get('trackingNumber', ''),
                    'tracking_url': shipment.get('trackingUrl', ''),
                    'created_at': shipment.get('creationDate', ''),
                    'updated_at': shipment.get('lastModified', ''),
                    'shipped_at': shipment.get('shippingDate', ''),
                    'delivery_date': shipment.get('deliveryDate', ''),
                    'shipping_method': shipment.get('shippingMethod', {}).get('name', ''),
                    'shipping_cost': shipment.get('shippingTotalPrice', 0),
                    'shipping_tax': shipment.get('shippingTotalTax', 0),
                    'gift': shipment.get('gift', False),
                    'gift_message': shipment.get('giftMessage', ''),
                    'shipping_address': shipment.get('shippingAddress', {}),
                    'line_items': [],
                    'source': 'additional_api_call'
                }
                
                # Extract line items for this additional shipment
                shipment_items = shipment.get('productItems', [])
                if not shipment_items:
                    shipment_items = shipment.get('productLineItems', [])
                if not shipment_items:
                    shipment_items = shipment.get('lineItems', [])
                if not shipment_items:
                    shipment_items = shipment.get('items', [])
                
                logging.info(f"Transform function - Additional Shipment {shipment.get('shipmentId', 'unknown')}: Found {len(shipment_items)} line items")
                
                # SFCC Logic for additional shipments: Find order line items that belong to this shipment
                # Use the same logic as main shipments
                additional_shipment_id = shipment.get('shipmentId', '')
                logging.info(f"🔍 Analyzing additional shipment {additional_shipment_id} (value: '{additional_shipment_id}')")
                
                additional_shipment_line_items = []
                for line_item in line_items:
                    line_item_shipment_id = line_item.get('shipment_id')
                    
                    if line_item_shipment_id == additional_shipment_id:
                        # Create shipment line item from order line item (same as main logic)
                        shipment_line = {
                            'id': line_item['id'],
                            'line_item_id': line_item['id'],
                            'item_id': line_item['id'],  # SFCC itemId for joining
                            'product_id': line_item.get('product_id', ''),
                            'quantity': line_item.get('quantity', 0),
                            'price': line_item.get('price', 0),
                            'sku': line_item.get('sku', ''),
                            'name': line_item.get('name', ''),
                            'join_method': 'additional_shipment_match',
                            # Add original SFCC identifiers for better joining
                            'sfcc_item_id': line_item.get('raw_line_item_data', {}).get('itemId', ''),
                            'sfcc_product_id': line_item.get('raw_line_item_data', {}).get('productId', ''),
                            'sfcc_shipment_id': line_item.get('raw_line_item_data', {}).get('shipmentId', '')
                        }
                        fulfillment['line_items'].append(shipment_line)
                        additional_shipment_line_items.append(line_item)
                        
                        # Update fulfillment status
                        line_item['quantity_shipped'] += line_item.get('quantity', 0)
                        line_item['fulfillment_status'] = 'fulfilled'
                        
                        logging.info(f"✅ Added line item {line_item['id']} to additional shipment {additional_shipment_id}")
                
                logging.info(f"Transform function - Additional Shipment {additional_shipment_id}: Contains {len(additional_shipment_line_items)} line items")
                
                fulfillments.append(fulfillment)
        
        # Skip returns and refunds - this eComm doesn't use them
        returns = []
        refunds = []
        
        # Update the transformed order with structured data
        transformed_order['lineItems'] = line_items
        transformed_order['fulfillments'] = fulfillments
        transformed_order['returns'] = returns
        transformed_order['refunds'] = refunds
        
        # Fix payment status at order level
        payment_status = sfcc_order.get('paymentStatus', 'not_paid')
        cybersource_status = ''
        payment_instruments = sfcc_order.get('paymentInstruments', [])
        if payment_instruments:
            cybersource_status = payment_instruments[0].get('paymentTransaction', {}).get('c_cybersourceStatus', '')
        
        order_shipping_status = sfcc_order.get('shippingStatus', '')
        
        # Determine actual payment status
        if cybersource_status == 'AUTHORIZED' and order_shipping_status == 'shipped':
            actual_payment_status = 'paid'
        elif cybersource_status == 'AUTHORIZED':
            actual_payment_status = 'authorized'
        else:
            actual_payment_status = payment_status
        
        # Override the incorrect SFCC paymentStatus
        transformed_order['paymentStatus'] = actual_payment_status
        transformed_order['original_sfcc_paymentStatus'] = payment_status
        transformed_order['payment_status_note'] = f"Corrected from SFCC '{payment_status}' using cybersource '{cybersource_status}'"
        
        # Remove raw SFCC sections since all data is now in transformed sections
        sections_to_remove = ['productItems', 'shipments', 'shippingItems']
        for section in sections_to_remove:
            if section in transformed_order:
                logging.info(f"Removing raw SFCC section '{section}' from output (data preserved in transformed sections)")
                del transformed_order[section]
        
        # Add summary counts
        transformed_order['line_items_count'] = len(line_items)
        transformed_order['fulfillments_count'] = len(fulfillments)
        transformed_order['returns_count'] = len(returns)
        transformed_order['refunds_count'] = len(refunds)
        
        # Add processing metadata
        transformed_order['data_structure_version'] = '1.0'
        transformed_order['transformed_at'] = datetime.now().isoformat()
        transformed_order['source_platform'] = 'salesforce_commerce_cloud'
        
        logging.info(f"Transformed order {order_id}: {len(line_items)} line items, {len(fulfillments)} fulfillments, {len(refunds)} refunds")
        return transformed_order
        
    except Exception as e:
        logging.error(f"Error transforming SFCC order data for {order_id}: {str(e)}")
        # Return original data if transformation fails
        return sfcc_order


def transform_sfcc_product_data(sfcc_product: dict) -> dict:
    """
    Transform SFCC Commerce API product data to include comprehensive variant and inventory information
    """
    # Determine if this is a master product
    product_type = sfcc_product.get('type', {})
    is_master = product_type.get('master', False)
    
    # Get master product images
    master_images = []
    main_image = sfcc_product.get('image', {})
    if main_image:
        master_images.append({
            'url': main_image.get('absUrl', ''),
            'alt': main_image.get('alt', {}).get('default', ''),
            'type': 'main'
        })
    
    # Add additional images from imageGroups (limit to 3)
    image_groups = sfcc_product.get('imageGroups', [])
    for group in image_groups:
        for img in group.get('images', [])[:3]:
            master_images.append({
                'url': img.get('absUrl', ''),
                'alt': img.get('alt', {}).get('default', ''),
                'type': group.get('viewType', 'additional')
            })
    
    # Debug: Log available keys to see what category/classification data is available
    logging.info(f"Product {sfcc_product.get('id', 'unknown')} keys: {list(sfcc_product.keys())}")
    
    # Extract categories and classification data - try multiple possible field names
    categories = []
    
    # Try different possible field names for categories
    category_data = (sfcc_product.get('categoryAssignments', []) or 
                    sfcc_product.get('categories', []) or 
                    sfcc_product.get('assignedCategories', []) or
                    sfcc_product.get('productCategories', []))
    
    logging.info(f"Product {sfcc_product.get('id', 'unknown')} category data: {category_data}")
    
    for assignment in category_data:
        if isinstance(assignment, dict):
            category_info = {
                'category_id': assignment.get('categoryId', assignment.get('id', '')),
                'category_name': assignment.get('categoryName', assignment.get('name', '')),
                'primary': assignment.get('primary', False)
            }
        else:
            # If it's just a string ID
            category_info = {
                'category_id': str(assignment),
                'category_name': '',
                'primary': False
            }
        categories.append(category_info)
    
    # Remove classifications processing since categories are working fine
    classifications = []
    
    # Extract weight information
    weight_info = {}
    weight = sfcc_product.get('weight', sfcc_product.get('c_weight', {}))
    if weight:
        if isinstance(weight, dict):
            weight_info = {
                'value': weight.get('value', 0),
                'unit': weight.get('unit', 'lb')
            }
        else:
            weight_info = {
                'value': weight,
                'unit': 'lb'  # Default unit
            }
    
    # Get creation and modification dates
    created_date = sfcc_product.get('creationDate', sfcc_product.get('c_creationDate', ''))
    updated_date = sfcc_product.get('lastModified', sfcc_product.get('modificationTime', ''))
    
    # Get price information from direct fields (site-specific data)
    price_value = sfcc_product.get('price', 0)
    currency = sfcc_product.get('priceCurrency', 'USD')
    price_per_unit = sfcc_product.get('pricePerUnit', 0)
    
    # Only return essential fields for clean output
    transformed_product = {
        'product_id': sfcc_product.get('id', ''),
        'name': sfcc_product.get('name', {}).get('default', '') if isinstance(sfcc_product.get('name'), dict) else sfcc_product.get('name', ''),
        'brand': sfcc_product.get('brand', ''),
        'description': sfcc_product.get('shortDescription', {}).get('default', {}).get('source', '') if isinstance(sfcc_product.get('shortDescription'), dict) else '',
        'is_master': is_master,
        'created_date': created_date,
        'updated_date': updated_date,
        'weight': weight_info,
        'price': price_value,
        'currency': currency,
        'categories': categories,
        'classifications': classifications,
        'images': master_images,
        'variants': [],
        'variant_count': 0
    }
    
    # Add basic inventory for master products only
    if is_master:
        # Use the actual fields returned by Salesforce with site context
        ats_value = sfcc_product.get('ats', 0)
        in_stock = sfcc_product.get('inStock', False)
        online = sfcc_product.get('online', False)
        
        # Debug logging for inventory data
        logging.info(f"Master {sfcc_product.get('id', 'unknown')} direct inventory: ats={ats_value}, inStock={in_stock}, online={online}")
        transformed_product['inventory'] = {
            "ats": ats_value,
            "in_stock": in_stock,
            "online": online,
            "orderable": online and (ats_value > 0 or in_stock),
            "stock_level": ats_value  # ATS is effectively the stock level
        }
    
    # Add basic pricing for master products only
    if is_master:
        price_model = sfcc_product.get('priceModel', {})
        transformed_product['pricing'] = {
            "currency": sfcc_product.get('currency', 'USD'),
            "price": price_model.get('price', price_model.get('priceBookPrice', 0))
        }
    
    # Extract comprehensive variant information
    variation_model = sfcc_product.get('variationModel', {})
    variants = []
    
    if variation_model:
        # Master product with variants
        variation_groups = variation_model.get('variationGroups', [])
        variants_data = variation_model.get('variants', [])
        
        for variant in variants_data:
            # Get variant availability and pricing models  
            variant_availability = variant.get('availabilityModel', {})
            variant_inventory = variant_availability.get('inventoryRecord', {})
            variant_price_model = variant.get('priceModel', {})
            
            # Get variant weight information
            variant_weight = variant.get('weight', variant.get('c_weight', {}))
            variant_weight_info = {}
            if variant_weight:
                if isinstance(variant_weight, dict):
                    variant_weight_info = {
                        'value': variant_weight.get('value', 0),
                        'unit': variant_weight.get('unit', 'lb')
                    }
                else:
                    variant_weight_info = {
                        'value': variant_weight,
                        'unit': 'lb'
                    }
            
            # Get variant dates - try multiple field names and inherit from master if not found
            variant_created = (variant.get('creationDate') or 
                             variant.get('c_creationDate') or 
                             variant.get('created') or
                             created_date)  # Inherit from master if not found in variant
            
            variant_updated = (variant.get('lastModified') or 
                             variant.get('modificationTime') or 
                             variant.get('updated') or
                             updated_date)  # Inherit from master if not found in variant
            
            # Debug logging for variant dates
            logging.info(f"Variant {variant.get('productId', 'unknown')} dates: created={variant_created}, updated={variant_updated}")
            logging.info(f"Variant {variant.get('productId', 'unknown')} available keys: {list(variant.keys())}")
            
            variant_info = {
                # === VARIANT ID MAPPING ===
                'variant_id': variant.get('productId', variant.get('id', '')),  # VARIANT ID (matches orders.variant_id)
                'sku': variant.get('productId', variant.get('id', '')),         # SKU (same as variant_id)
                'product_id': sfcc_product.get('id', ''),                               # MASTER PRODUCT ID (matches orders.product_id)
                'master_product_id': sfcc_product.get('id', ''),                        # Explicit master reference
                'belongs_to_master': sfcc_product.get('id', ''),                        # Clear relationship field
                'name': variant.get('name', variant.get('productName', '')),
                'brand': variant.get('brand', sfcc_product.get('brand', '')),           # Inherit from master if not present
                'created_date': variant_created,
                'updated_date': variant_updated,
                'weight': variant_weight_info,
                'price': variant.get('price', 0),  # Use direct price field
                'currency': variant.get('priceCurrency', 'USD'),  # Use direct currency field
                'variation_values': variant.get('variationValues', {}),
                'inventory': {
                    'ats': variant.get('ats', 0),  # Use direct ATS field
                    'in_stock': variant.get('inStock', False),  # Use direct inStock field
                    'online': variant.get('online', False),  # Use direct online field
                    'orderable': variant.get('online', False) and (variant.get('ats', 0) > 0 or variant.get('inStock', False)),
                    'stock_level': variant.get('ats', 0)  # ATS is the stock level
                }
            }
            variants.append(variant_info)
        
        # Add variation attributes information
        transformed_product['variation_attributes'] = variation_model.get('variationAttributes', [])
        transformed_product['variation_groups'] = variation_groups
        
        transformed_product['variants'] = variants
        transformed_product['variant_count'] = len(variants)
    
    return transformed_product


def save_to_datalake(data: dict, datalake_key: str, path: str, filename: str = None) -> bool:
    """
    Save data to Azure Data Lake Storage
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
            filename = f"salesforce_orders.{timestamp}-orders"
        
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


def fetch_salesforce_products(access_token: str, base_url: str, organization_id: str, site_id: str, page_size: str, catalog_id: str = None) -> dict:
    """
    Fetch products from Salesforce Commerce Cloud Product Search API
    """
    try:
        # Build the API URL
        api_path = f"/product/products/v1"
        url = f"{base_url}{api_path}/organizations/{organization_id}/product-search"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Prepare query parameters with site context for inventory/pricing
        search_query = {
            "limit": int(page_size),
            "query": {
                "textQuery": {
                    "fields": [
                        "id", "name"
                    ],
                    "searchPhrase": "*"
                }
            },
            "offset": 0,
            "expand": [
                "availability", "images", "prices", "categories"
            ]
        }
        
        # Add site-specific parameters for inventory and pricing data
        params = {
            'siteId': site_id
        }

        # Add catalog_id refinement if provided
        # if catalog_id:
        #     search_query['refinements'] = [
        #         {
        #             "attributeId": "catalogId",
        #             "values": [catalog_id]
        #         }
        #     ]
        
        logging.info(f"Fetching products from Salesforce Commerce Cloud: {url}")
        logging.info(f"Search query: {json.dumps(search_query, indent=2)}")
        
        all_products = []
        debug_info = {
            'masters_found': [],
            'variants_found': [],
            'variant_matching': [],
            'masters_with_variants': {}
        }
        offset = 0
        max_pages = 50  # Safety limit
        page_count = 0
        
        while page_count < max_pages:
            page_count += 1
            search_query["offset"] = offset
            
            logging.info(f"Fetching page {page_count}, offset {offset}")
            
            response = requests.post(url, headers=headers, json=search_query, params=params, timeout=60)
            
            if response.status_code != 200:
                logging.error(f"Salesforce API error: {response.status_code}")
                logging.error(f"Response: {response.text}")
                return {
                    "error": "API_ERROR",
                    "message": f"Salesforce API returned status {response.status_code}",
                    "details": response.text
                }
            
            data = response.json()
            products = data.get('hits', [])
            
            if not products:
                logging.info("No more products found, ending pagination")
                break
                
            # Process products and organize masters with nested variants
            masters_dict = {}
            variant_products = []
            
            # Pass 1: Process all master products
            for product in products:
                product_type = product.get('type', {})
                is_master = product_type.get('master', False)
                product_id = product.get('id', '')
                
                if is_master:
                    # Transform master product
                    transformed_master = transform_sfcc_product_data(product)
                    masters_dict[product_id] = transformed_master
                    debug_info['masters_found'].append(product_id)
                else:
                    # Store variants for second pass
                    variant_products.append(product)
                    debug_info['variants_found'].append(product_id)
            
            # Pass 2: Process variants and nest them under masters
            for product in variant_products:
                variant_id = product.get('id', '')
                
                # Find the master this variant belongs to
                master_id = None
                for master_key in masters_dict.keys():
                    # Extract base pattern from master (remove XXXX)
                    master_base = master_key.replace('XXXX', '')
                    # Check if variant starts with this base pattern
                    if variant_id.startswith(master_base):
                        master_id = master_key
                        break
                
                if master_id and master_id in masters_dict:
                    # Transform variant data
                    availability_model = product.get('availabilityModel', {})
                    inventory_record = availability_model.get('inventoryRecord', {})
                    
                    # Try alternative inventory field names for variants
                    variant_inventory_data = (product.get('inventory', {}) or 
                                            product.get('inventoryRecord', {}) or
                                            product.get('stockInfo', {}))
                    
                    # Debug logging for variant inventory data
                    logging.info(f"Variant {variant_id} availability_model: {availability_model}")
                    logging.info(f"Variant {variant_id} inventory_record: {inventory_record}")
                    logging.info(f"Variant {variant_id} variant_inventory_data: {variant_inventory_data}")
                    
                    # Get variant images
                    variant_images = []
                    main_image = product.get('image', {})
                    if main_image:
                        variant_images.append({
                            'url': main_image.get('absUrl', ''),
                            'alt': main_image.get('alt', {}).get('default', ''),
                            'type': 'main'
                        })
                    
                    # Add additional images from imageGroups
                    image_groups = product.get('imageGroups', [])
                    for group in image_groups:
                        for img in group.get('images', [])[:3]:  # Limit to 3 additional images
                            variant_images.append({
                                'url': img.get('absUrl', ''),
                                'alt': img.get('alt', {}).get('default', ''),
                                'type': group.get('viewType', 'additional')
                            })
                    
                    # Get variant dates and other fields
                    variant_created = product.get('creationDate', product.get('c_creationDate', ''))
                    variant_updated = product.get('lastModified', product.get('modificationTime', ''))
                    
                    # Get variant weight
                    variant_weight = product.get('weight', product.get('c_weight', {}))
                    variant_weight_info = {}
                    if variant_weight:
                        if isinstance(variant_weight, dict):
                            variant_weight_info = {
                                'value': variant_weight.get('value', 0),
                                'unit': variant_weight.get('unit', 'lb')
                            }
                        else:
                            variant_weight_info = {
                                'value': variant_weight,
                                'unit': 'lb'
                            }
                    
                    # Get variant price from direct fields
                    variant_price = product.get('price', 0)
                    variant_currency = product.get('priceCurrency', 'USD')
                    
                    # Create variant data structure
                    variant_data = {
                        'variant_id': variant_id,
                        'sku': variant_id,
                        'name': product.get('name', {}).get('default', '') if isinstance(product.get('name'), dict) else product.get('name', ''),
                        'brand': product.get('brand', ''),
                        'created_date': variant_created,
                        'updated_date': variant_updated,
                        'weight': variant_weight_info,
                        'price': variant_price,
                        'currency': variant_currency,
                        'upc': product.get('upc', ''),
                        'manufacturer_sku': product.get('manufacturerSku', ''),
                        'belongs_to_master': master_id,
                        'variation_values': {
                            'color': product.get('c_color', ''),
                            'size': product.get('c_size', ''),
                            'style': product.get('c_style', '')
                        },
                        'images': variant_images,
                        'inventory': {
                            'ats': product.get('ats', 0),
                            'in_stock': product.get('inStock', False),
                            'online': product.get('online', False),
                            'orderable': product.get('online', False) and (product.get('ats', 0) > 0 or product.get('inStock', False)),
                            'stock_level': product.get('ats', 0)
                        }
                    }
                    
                    # Add variant to master
                    masters_dict[master_id]['variants'].append(variant_data)
                    masters_dict[master_id]['variant_count'] = len(masters_dict[master_id]['variants'])
                    
                    # Track debug info
                    debug_info['variant_matching'].append({
                        'variant_id': variant_id,
                        'matched_to_master': master_id,
                        'match_successful': True
                    })
                    
                    if master_id not in debug_info['masters_with_variants']:
                        debug_info['masters_with_variants'][master_id] = []
                    debug_info['masters_with_variants'][master_id].append(variant_id)
                else:
                    # Variant couldn't be matched to a master
                    debug_info['variant_matching'].append({
                        'variant_id': variant_id,
                        'matched_to_master': None,
                        'match_successful': False
                    })
            
            # Add only master products (with nested variants) to results
            all_products.extend(list(masters_dict.values()))
            logging.info(f"Retrieved {len(products)} products, total: {len(all_products)}")
            
            # Check if we have more pages using Commerce API pagination
            total = data.get('total', 0)
            if offset + len(products) >= total:
                logging.info(f"Reached end of results: {offset + len(products)} >= {total}")
                break
                
            offset += len(products)
        
        # Note: Separate inventory/pricing APIs returned 404, so inventory/pricing data 
        # must be available through the main product API with proper site context
        logging.info("Skipping separate API calls - using site-specific product data for inventory/pricing")
        # Format response data
        final_data = {
            "data": all_products,
            "total_count": len(all_products),
            "debug_info": debug_info,
            "metadata": {
                "source": url,
                "organization_id": organization_id,
                "site_id": site_id,
                "item_type": "products_combined",
                "includes": ["products", "inventory", "pricing", "promotions"],
                "page_size": page_size,
                "pages_fetched": page_count,
                "timestamp": datetime.now().isoformat(),
                "note": "Products with separate inventory and pricing API integration"
            }
        }
        
        logging.info(f"Successfully fetched {len(all_products)} products with integrated inventory and pricing data")
        return final_data

    except Exception as e:
        logging.error(f"Error fetching Salesforce products: {str(e)}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "error": "FETCH_ERROR",
            "message": f"Failed to fetch products: {str(e)}",
            "traceback": traceback.format_exc()
        }


def fetch_salesforce_inventory(access_token: str, base_url: str, organization_id: str, site_id: str, page_size: str) -> dict:
    """
    Fetch inventory data from Salesforce Commerce Cloud using the same product search API but focused on inventory fields
    """
    try:
        # Try using the inventory-specific API endpoint
        api_path = f"/product/inventory/v1"
        url = f"{base_url}{api_path}/organizations/{organization_id}/inventory-lists/inventory/product-inventory-records"
        
        logging.info(f"Inventory API URL: {url}")
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Use GET request for inventory API with query parameters
        params = {
            'limit': int(page_size),
            'offset': 0
        }
        
        logging.info(f"Inventory API params: {params}")
        logging.info(f"Fetching inventory from Salesforce Commerce Cloud: {url}")
        
        all_inventory = []
        offset = 0
        max_pages = 50
        page_count = 0
        
        while page_count < max_pages:
            page_count += 1
            params["offset"] = offset
            
            response = requests.get(url, headers=headers, params=params, timeout=60)
            
            if response.status_code != 200:
                logging.error(f"Salesforce API error: {response.status_code}")
                return {
                    "error": "API_ERROR",
                    "message": f"Salesforce API returned status {response.status_code}",
                    "details": response.text
                }
            
            data = response.json()
            inventory_items = data.get('hits', [])
            
            if not inventory_items:
                break
                
            all_inventory.extend(inventory_items)
            
            total = data.get('total', 0)
            if offset + len(inventory_items) >= total:
                break
                
            offset += len(inventory_items)
        
        final_data = {
            "data": all_inventory,
            "total_count": len(all_inventory),
            "metadata": {
                "source": url,
                "organization_id": organization_id,
                "site_id": site_id,
                "item_type": "inventory",
                "page_size": page_size,
                "pages_fetched": page_count,
                "timestamp": datetime.now().isoformat()
            }
        }
        
        return final_data

    except Exception as e:
        logging.error(f"Error fetching Salesforce inventory: {str(e)}")
        return {
            "error": "FETCH_ERROR",
            "message": f"Failed to fetch inventory: {str(e)}"
        }


def fetch_salesforce_pricing(access_token: str, base_url: str, organization_id: str, site_id: str, page_size: str, price_book_id: str = None) -> dict:
    """
    Fetch pricing data from Salesforce Commerce Cloud
    """
    try:
        # Try using the pricing-specific API endpoint
        api_path = f"/pricing/products/v1"
        url = f"{base_url}{api_path}/organizations/{organization_id}/product-prices"
        
        logging.info(f"Pricing API URL: {url}")
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Pricing-focused search query - simplified to ensure it works
        search_query = {
            "limit": int(page_size),
            "query": {
                "textQuery": {
                    "fields": ["id", "name"],
                    "searchPhrase": "*"
                }
            },
            "offset": 0,
            "expand": ["prices"]
        }
        
        logging.info(f"Pricing search query: {search_query}")
        
        logging.info(f"Fetching pricing from Salesforce Commerce Cloud: {url}")
        
        all_pricing = []
        offset = 0
        max_pages = 50
        page_count = 0
        
        while page_count < max_pages:
            page_count += 1
            search_query["offset"] = offset
            
            response = requests.post(url, headers=headers, json=search_query, timeout=60)
            
            if response.status_code != 200:
                logging.error(f"Salesforce API error: {response.status_code}")
                return {
                    "error": "API_ERROR",
                    "message": f"Salesforce API returned status {response.status_code}",
                    "details": response.text
                }
            
            data = response.json()
            pricing_items = data.get('hits', [])
            
            if not pricing_items:
                break
                
            all_pricing.extend(pricing_items)
            
            total = data.get('total', 0)
            if offset + len(pricing_items) >= total:
                break
                
            offset += len(pricing_items)
        
        final_data = {
            "data": all_pricing,
            "total_count": len(all_pricing),
            "metadata": {
                "source": url,
                "organization_id": organization_id,
                "site_id": site_id,
                "item_type": "pricing",
                "page_size": page_size,
                "pages_fetched": page_count,
                "timestamp": datetime.now().isoformat()
            }
        }
        
        return final_data

    except Exception as e:
        logging.error(f"Error fetching Salesforce pricing: {str(e)}")
        return {
            "error": "FETCH_ERROR",
            "message": f"Failed to fetch pricing: {str(e)}"
        }
