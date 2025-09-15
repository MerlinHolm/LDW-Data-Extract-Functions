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
        
        # Get other parameters with defaults
        base_url = req.params.get('base_url', 'kv7kzm78.api.commercecloud.salesforce.com')
        
        # Add https:// prefix if not present
        if not base_url.startswith('http://') and not base_url.startswith('https://'):
            base_url = f'https://{base_url}'
        api_version = req.params.get('api_version', 'v1')
        organization_id = req.params.get('organization_id', 'f_ecom_zysr_001')
        site_id = req.params.get('site_id', 'RefArchGlobal')
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
        access_token = get_salesforce_access_token(client_id, client_secret)
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
        
        # Get other parameters with defaults
        base_url = req.params.get('base_url', 'kv7kzm78.api.commercecloud.salesforce.com')
        
        # Add https:// prefix if not present
        if not base_url.startswith('http://') and not base_url.startswith('https://'):
            base_url = f'https://{base_url}'
        api_version = req.params.get('api_version', 'v1')
        organization_id = req.params.get('organization_id', 'f_ecom_zysr_001')
        site_id = req.params.get('site_id', 'RefArchUS')
        limit = req.params.get('limit', '200')
        data_lake_path = req.params.get('data_lake_path', 'RetailOrders/input/files/json/orders')
        filename_prefix = req.params.get('filename', 'orders')
        start_date = req.params.get('start_date')
        end_date = req.params.get('end_date')
        
        # Step 1: Get OAuth2 access token
        access_token = get_salesforce_access_token(client_id, client_secret)
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
            debug_url = f"{base_url}/checkout/orders/{api_version}/organizations/{organization_id}/orders?siteId={site_id}&limit={limit}"
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


def get_salesforce_access_token(client_id: str, client_secret: str) -> str:
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
        
        data = {
            'grant_type': 'client_credentials',
            'scope': 'SALESFORCE_COMMERCE_API:zysr_001 sfcc.orders.rw sfcc.products'
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
    Fetch orders, lines, and shipments from Salesforce Commerce Cloud
    """
    try:
        # Build the API URL
        api_path = f"/checkout/orders/{api_version}"
        url = f"{base_url}{api_path}/organizations/{organization_id}/orders"
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Prepare query parameters
        params = {
            'siteId': site_id,
            'count': limit,
            'start': 0,
            'expand': 'payments,paymentInstruments'  # Attempt to get payment and refund info
        }


        # Add date filters if provided
        if start_date and end_date:
            params['creationDateFrom'] = start_date
            params['creationDateTo'] = end_date
        
        logging.info(f"Fetching orders from Salesforce Commerce Cloud: {url}")
        logging.info(f"API Headers: {headers}")
        logging.info(f"API Params: {params}")
        
        all_orders = []
        offset = 0
        max_pages = 10  # Safety limit to prevent infinite loops
        page_count = 0
        
        while page_count < max_pages:
            # Add offset for pagination
            current_params = params.copy()
            current_params['offset'] = offset
            page_count += 1
            
            logging.info(f"Making API call - Page {page_count}, Offset: {offset}")
            
            response = requests.get(url, headers=headers, params=current_params, timeout=30)
            
            logging.info(f"API Response Status: {response.status_code}")
            logging.info(f"API Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                orders = data.get('data', [])
                
                logging.info(f"API Response Data Keys: {list(data.keys()) if data else 'None'}")
                
                if not orders:
                    logging.info("No orders found in response, breaking pagination loop")
                    break
                    
                all_orders.extend(orders)
                logging.info(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                
                # Check if there are more pages
                if len(orders) < int(limit):
                    logging.info(f"Received {len(orders)} orders, less than limit {limit}. Ending pagination.")
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
                    "request_params": current_params
                }
        
        result = {
            'data': all_orders,
            'total_count': len(all_orders),
            'fetch_timestamp': datetime.now().isoformat()
        }
        
        if page_count >= max_pages:
            logging.warning(f"Reached maximum page limit ({max_pages}). May have more data available.")
        
        logging.info(f"Successfully fetched {len(all_orders)} orders from Salesforce in {page_count} pages")
        return result
        
    except Exception as e:
        logging.error(f"Error fetching Salesforce orders: {str(e)}")
        return None


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
        
        # Enhanced product search query with comprehensive fields
        search_query = {
            "limit": int(page_size),
            "query": {
                "textQuery": {
                    "fields": [
                        # Basic queryable fields only
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
                
                # Temporarily disabled pricing and promotion processing
                # # Extract and organize pricing data
                # price_model = product.get('priceModel', {})
                # price_info = price_model.get('priceInfo', {})
                # price_range = price_model.get('priceRange', {})
                
                # product['pricing'] = {
                #     "currency": product.get('currency', 'USD'),
                #     "price": price_model.get('price', 0),
                #     "price_book": price_info.get('priceBook'),
                #     "price_book_price": price_model.get('priceBookPrice', 0),
                #     "min_price": price_range.get('minPrice', 0),
                #     "max_price": price_range.get('maxPrice', 0),
                #     "price_tiers": price_range.get('priceTiers', []),
                #     "tiered_prices": price_model.get('tieredPrices', [])
                # }
                
                # # Extract promotions
                # promotions = product.get('promotions', [])
                # product_promotions = product.get('productPromotions', [])
                
                # product['promotions'] = {
                #     "active_promotions": promotions,
                #     "product_promotions": product_promotions,
                #     "promotional_price": next((p.get('promotionalPrice') for p in promotions if p.get('promotionalPrice')), None),
                #     "callout_message": next((p.get('calloutMsg') for p in promotions if p.get('calloutMsg')), None)
                # }
            
            all_products.extend(products)
            logging.info(f"Retrieved {len(products)} products, total: {len(all_products)}")
            
            # Check if we have more pages
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
