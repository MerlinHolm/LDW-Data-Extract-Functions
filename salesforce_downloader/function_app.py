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
            params['creationDateFrom'] = start_date
            params['creationDateTo'] = end_date
        
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
                    break
                
                # Enhance each order with individual order details if needed
                enhanced_orders = []
                for order in orders:
                    try:
                        # Get individual order details if we have an order number/ID
                        order_id = order.get('orderNo') or order.get('id') or order.get('orderNumber')
                        if order_id:
                            detailed_order = fetch_individual_order(access_token, base_url, api_version, organization_id, site_id, order_id)
                            if detailed_order:
                                enhanced_orders.append(detailed_order)
                            else:
                                enhanced_orders.append(order)  # Use original if detailed fetch fails
                        else:
                            enhanced_orders.append(order)
                    except Exception as e:
                        logging.warning(f"Failed to enhance order {order.get('orderNo', 'unknown')}: {str(e)}")
                        enhanced_orders.append(order)  # Use original if enhancement fails
                
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
        params = {
            'siteId': site_id,
            'expand': 'productItems,payments,paymentInstruments,shipments,notes'  # Request all related data
        }
        
        logging.info(f"Fetching comprehensive order data for {order_id} from: {url}")
        logging.info(f"Expand parameters: {params['expand']}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            order_data = response.json()
            
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
        product_items = sfcc_order.get('productItems', [])
        
        for idx, item in enumerate(product_items):
            line_item = {
                'id': item.get('itemId', f"{order_id}_line_{idx}"),
                'order_id': order_id,
                'product_id': item.get('productId'),
                'variant_id': item.get('productId'),  # SFCC uses productId as variant
                'sku': item.get('productId'),
                'name': item.get('productName', ''),
                'quantity': item.get('quantity', 0),
                'price': item.get('price', 0),
                'base_price': item.get('basePrice', 0),
                'price_after_item_discount': item.get('priceAfterItemDiscount', 0),
                'price_after_order_discount': item.get('priceAfterOrderDiscount', 0),
                'tax': item.get('tax', 0),
                'item_text': item.get('itemText', ''),
                'gift': item.get('gift', False),
                'gift_message': item.get('giftMessage', ''),
                'inventory_id': item.get('inventoryId', ''),
                'bonus_product_line_item': item.get('bonusProductLineItem', False),
                'bundled_product_line_item': item.get('bundledProductLineItem', False),
                'option_product_line_item': item.get('optionProductLineItem', False),
                'product_list_item': item.get('productListItem', False),
                'shipment_id': item.get('shipmentId', ''),
                # Add fulfillment status tracking
                'fulfillment_status': 'unfulfilled',  # Will be updated based on shipments
                'quantity_fulfilled': 0,
                'quantity_shipped': 0,
                'quantity_returned': 0
            }
            
            # Add any custom attributes
            if 'c_customAttributes' in item:
                line_item['custom_attributes'] = item['c_customAttributes']
                
            line_items.append(line_item)
        
        # Extract and transform shipments (fulfillments)
        fulfillments = []
        shipments = sfcc_order.get('shipments', [])
        
        for shipment in shipments:
            fulfillment = {
                'id': shipment.get('shipmentId', ''),
                'order_id': order_id,
                'status': shipment.get('status', ''),
                'tracking_company': shipment.get('trackingNumber', ''),  # SFCC may not separate company
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
                # Shipping address
                'shipping_address': shipment.get('shippingAddress', {}),
                # Line items in this shipment
                'line_items': []
            }
            
            # Extract line items for this shipment
            shipment_items = shipment.get('productItems', [])
            for item in shipment_items:
                shipment_line = {
                    'id': item.get('itemId', ''),
                    'line_item_id': item.get('itemId', ''),
                    'product_id': item.get('productId', ''),
                    'quantity': item.get('quantity', 0),
                    'price': item.get('price', 0)
                }
                fulfillment['line_items'].append(shipment_line)
                
                # Update line item fulfillment status
                for line_item in line_items:
                    if line_item['id'] == item.get('itemId', ''):
                        line_item['quantity_shipped'] += item.get('quantity', 0)
                        line_item['shipment_id'] = shipment.get('shipmentId', '')
                        if line_item['quantity_shipped'] >= line_item['quantity']:
                            line_item['fulfillment_status'] = 'fulfilled'
                        else:
                            line_item['fulfillment_status'] = 'partial'
            
            fulfillments.append(fulfillment)
        
        # Extract returns and refunds (if available in SFCC data)
        returns = []
        refunds = []
        
        # SFCC may include return information in payments or separate return objects
        payments = sfcc_order.get('payments', [])
        for payment in payments:
            if payment.get('paymentMethodId') == 'CREDIT' or payment.get('amount', 0) < 0:
                # This might be a refund
                refund = {
                    'id': payment.get('paymentId', ''),
                    'order_id': order_id,
                    'amount': abs(payment.get('amount', 0)),
                    'currency': payment.get('currencyCode', 'USD'),
                    'reason': payment.get('paymentMethodId', ''),
                    'created_at': payment.get('creationDate', ''),
                    'processed_at': payment.get('creationDate', ''),
                    'gateway': payment.get('paymentProcessor', ''),
                    'transaction_id': payment.get('paymentTransactionId', ''),
                    'note': payment.get('c_note', ''),
                    'refund_line_items': []  # Would need additional API calls to get line-level returns
                }
                refunds.append(refund)
        
        # Update the transformed order with structured data
        transformed_order.update({
            'lineItems': line_items,
            'fulfillments': fulfillments,
            'returns': returns,
            'refunds': refunds,
            # Add summary counts
            'line_items_count': len(line_items),
            'fulfillments_count': len(fulfillments),
            'returns_count': len(returns),
            'refunds_count': len(refunds),
            # Add processing metadata
            'data_structure_version': '1.0',
            'transformed_at': datetime.now().isoformat(),
            'source_platform': 'salesforce_commerce_cloud'
        })
        
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
    try:
        # Start with the base product data
        transformed_product = sfcc_product.copy()
        
        # Commerce API structure - extract availability data
        availability_model = sfcc_product.get('availabilityModel', {})
        inventory_record = availability_model.get('inventoryRecord', {})
        
        # Comprehensive inventory information
        transformed_product['inventory'] = {
            "orderable": availability_model.get('orderable', False),
            "in_stock": availability_model.get('inStock', False),
            "allocation": inventory_record.get('allocation', 0),
            "preorderable": inventory_record.get('preorderable', False),
            "backorderable": inventory_record.get('backorderable', False),
            "stock_level": inventory_record.get('stockLevel', 0),
            "ats": inventory_record.get('ats', 0),  # Available to Sell
            "reservations": inventory_record.get('reservations', 0),
            "turnover": inventory_record.get('turnover', 0),
            "perpetual": inventory_record.get('perpetual', False),
            "preorder_backorder_allocation": inventory_record.get('preorderBackorderAllocation', 0),
            "preorder_backorder_handling": inventory_record.get('preorderBackorderHandling', ''),
            "in_stock_date": inventory_record.get('inStockDate', ''),
            "restockable": inventory_record.get('restockable', False)
        }
        
        # Extract and organize pricing data
        price_model = sfcc_product.get('priceModel', {})
        price_info = price_model.get('priceInfo', {})
        price_range = price_model.get('priceRange', {})
        
        transformed_product['pricing'] = {
            "currency": sfcc_product.get('currency', 'USD'),
            "price": price_model.get('price', 0),
            "price_book": price_info.get('priceBook'),
            "price_book_price": price_model.get('priceBookPrice', 0),
            "min_price": price_range.get('minPrice', 0),
            "max_price": price_range.get('maxPrice', 0),
            "price_tiers": price_range.get('priceTiers', []),
            "tiered_prices": price_model.get('tieredPrices', []),
            "sale_price": price_model.get('salePrice', 0),
            "list_price": price_model.get('listPrice', 0)
        }
        
        # Extract comprehensive variant information
        variation_model = sfcc_product.get('variationModel', {})
        variants = []
        
        if variation_model:
            # Master product with variants
            variation_groups = variation_model.get('variationGroups', [])
            variants_data = variation_model.get('variants', [])
            
            for variant in variants_data:
                variant_info = {
                    'variant_id': variant.get('productId', ''),
                    'sku': variant.get('productId', ''),
                    'orderable': variant.get('orderable', False),
                    'price': variant.get('price', 0),
                    'variation_values': variant.get('variationValues', {}),
                    'inventory': {
                        'stock_level': variant.get('stockLevel', 0),
                        'ats': variant.get('ats', 0),
                        'orderable': variant.get('orderable', False),
                        'in_stock': variant.get('inStock', False)
                    }
                }
                variants.append(variant_info)
            
            # Add variation attributes information
            transformed_product['variation_attributes'] = variation_model.get('variationAttributes', [])
            transformed_product['variation_groups'] = variation_groups
        
        transformed_product['variants'] = variants
        transformed_product['is_master'] = len(variants) > 0
        transformed_product['variant_count'] = len(variants)
        
        # Extract promotions
        promotions = sfcc_product.get('promotions', [])
        product_promotions = sfcc_product.get('productPromotions', [])
        
        transformed_product['promotions'] = {
            "active_promotions": promotions,
            "product_promotions": product_promotions,
            "promotional_price": next((p.get('promotionalPrice') for p in promotions if p.get('promotionalPrice')), None),
            "callout_message": next((p.get('calloutMsg') for p in promotions if p.get('calloutMsg')), None)
        }
        
        # Extract images
        image_groups = sfcc_product.get('imageGroups', [])
        transformed_product['images'] = []
        for group in image_groups:
            for image in group.get('images', []):
                transformed_product['images'].append({
                    'view_type': group.get('viewType', ''),
                    'alt': image.get('alt', ''),
                    'dis_base_link': image.get('disBaseLink', ''),
                    'link': image.get('link', ''),
                    'title': image.get('title', '')
                })
        
        # Add comprehensive product metadata
        transformed_product.update({
            # Product identifiers - In Commerce API, use id as SKU
            'sku': sfcc_product.get('id', ''),  # Commerce API id is the SKU
            'product_id': sfcc_product.get('id', ''),
            'manufacturer_name': sfcc_product.get('manufacturerName', ''),
            'manufacturer_sku': sfcc_product.get('manufacturerSku', ''),
            'upc': sfcc_product.get('upc', ''),
            'ean': sfcc_product.get('ean', ''),
            'isbn': sfcc_product.get('isbn', ''),
            
            # Product details
            'brand': sfcc_product.get('brand', ''),
            
            # Status flags
            'online': sfcc_product.get('online', False),
            'searchable': sfcc_product.get('searchable', False),
            
            # Descriptions
            'short_description': sfcc_product.get('shortDescription', ''),
            'long_description': sfcc_product.get('longDescription', ''),
            
            # Processing metadata
            'data_structure_version': '1.0',
            'transformed_at': datetime.now().isoformat(),
            'source_platform': 'salesforce_commerce_cloud'
        })
        
        logging.info(f"Transformed product {sfcc_product.get('id', 'unknown')}: {len(variants)} variants, inventory ATS: {transformed_product['inventory']['ats']}")
        return transformed_product
        
    except Exception as e:
        logging.error(f"Error transforming SFCC product data for {sfcc_product.get('id', 'unknown')}: {str(e)}")
        # Return original data if transformation fails
        return sfcc_product


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
        
        # Basic product search query
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
                "availability", "images", "prices"
            ]
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
        offset = 0
        max_pages = 50  # Safety limit
        page_count = 0
        
        while page_count < max_pages:
            page_count += 1
            search_query["offset"] = offset
            
            logging.info(f"Fetching page {page_count}, offset {offset}")
            
            response = requests.post(url, headers=headers, json=search_query, timeout=60)
            
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
                
            # Process each product to organize inventory and pricing data
            for product in products:
                # Extract and organize inventory data
                availability_model = product.get('availabilityModel', {})
                inventory_record = availability_model.get('inventoryRecord', {})
                
                product['inventory'] = {
                    "orderable": availability_model.get('orderable', False),
                    "in_stock": availability_model.get('inStock', False),
                    "allocation": inventory_record.get('allocation', 0),
                    "preorderable": inventory_record.get('preorderable', False),
                    "backorderable": inventory_record.get('backorderable', False),
                    "stock_level": inventory_record.get('stockLevel', 0),
                    "ats": inventory_record.get('ats', 0),
                    "reservations": inventory_record.get('reservations', 0),
                    "turnover": inventory_record.get('turnover', 0)
                }
            
            all_products.extend(products)
            logging.info(f"Retrieved {len(products)} products, total: {len(all_products)}")
            
            # Check if we have more pages using Commerce API pagination
            total = data.get('total', 0)
            if offset + len(products) >= total:
                logging.info(f"Reached end of results: {offset + len(products)} >= {total}")
                break
                
            offset += len(products)
        
        # Format response data
        final_data = {
            "data": all_products,
            "total_count": len(all_products),
            "metadata": {
                "source": url,
                "organization_id": organization_id,
                "site_id": site_id,
                "item_type": "products_combined",
                "includes": ["products", "inventory", "pricing", "promotions"],
                "page_size": page_size,
                "pages_fetched": page_count,
                "timestamp": datetime.now().isoformat(),
                "note": "Products with organized inventory, pricing, and promotion data"
            }
        }
        
        logging.info(f"Successfully fetched {len(all_products)} products from Salesforce")
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
    Fetch inventory data from Salesforce Commerce Cloud
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
        
        # Inventory-focused search query
        search_query = {
            "limit": int(page_size),
            "query": {
                "textQuery": {
                    "fields": [
                        "id", "inventoryRecord.allocation", "inventoryRecord.preorderable", 
                        "inventoryRecord.backorderable", "inventoryRecord.stockLevel", "inventoryRecord.ats",
                        "inventoryRecord.reservations", "inventoryRecord.turnover", "availabilityModel.orderable",
                        "availabilityModel.inStock", "availabilityModel.inventoryRecord"
                    ],
                    "searchPhrase": "*"
                }
            },
            "offset": 0,
            "expand": ["availability", "inventory"],
            "select": "(id,inventoryRecord,availabilityModel)"
        }
        
        logging.info(f"Fetching inventory from Salesforce Commerce Cloud: {url}")
        
        all_inventory = []
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
        # Build the API URL
        api_path = f"/product/products/v1"
        url = f"{base_url}{api_path}/organizations/{organization_id}/product-search"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Pricing-focused search query
        search_query = {
            "limit": int(page_size),
            "query": {
                "textQuery": {
                    "fields": [
                        "id", "currency", "priceModel.price", "priceModel.priceInfo.price",
                        "priceModel.priceInfo.priceBook", "priceModel.priceBook", "priceModel.priceBookPrice",
                        "priceModel.priceRange.maxPrice", "priceModel.priceRange.minPrice", "priceModel.priceRange.priceTiers",
                        "priceModel.tieredPrices", "promotions.promotionalPrice", "promotions.calloutMsg", "productPromotions"
                    ],
                    "searchPhrase": "*"
                }
            },
            "offset": 0,
            "expand": ["prices", "promotions", "price_books"],
            "select": "(id,currency,priceModel,promotions,productPromotions)"
        }
        
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
