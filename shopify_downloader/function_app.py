import azure.functions as func
import json
import logging
import requests
import re
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="get_product_data")
def get_product_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Shopify product data download function processed a request.')

    try:
        # Get parameters from request
        auth_token = req.params.get('auth_token')
        base_url = req.params.get('base_url')  # e.g., "dearfoams-costco-next"
        api_version = req.params.get('api_version', '2024-10')
        filename_prefix = req.params.get('filename', 'shopify')
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'RetailProducts/input/files/json/products/base')
        page_size = req.params.get('page_size', '250')

        # Validate required parameters
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        if not base_url:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "base_url parameter is required (e.g., 'dearfoams-costco-next')"}),
                status_code=400,
                mimetype="application/json"
            )

        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "datalake_key parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Construct full Shopify GraphQL URL
        # https://dearfoams-costco-next.myshopify.com/admin/api/2024-10/graphql.json
        full_base_url = f"https://{base_url}.myshopify.com/admin/api/{api_version}/graphql.json"
        
        logging.info(f"Fetching Shopify product data from: {full_base_url}")
        logging.info(f"Page size: {page_size}")
        logging.info(f"Data Lake path: {data_lake_path}")
        logging.info(f"Filename prefix: {filename_prefix}")

        # Fetch Shopify product data
        product_data = fetch_shopify_products(auth_token, full_base_url, page_size)

        # Check for errors
        items_list = product_data.get('data', [])
        has_items = len(items_list) > 0
        has_errors = 'error' in product_data

        if has_errors:
            # Return error without saving any file
            return func.HttpResponse(
                json.dumps(product_data),
                status_code=500,
                mimetype="application/json"
            )

        if not has_items:
            # Create empty file when no items found (but no errors)
            empty_data = {
                "data": [],
                "total_count": 0,
                "metadata": {
                    "source": full_base_url,
                    "api_version": api_version,
                    "item_type": "products",
                    "page_size": page_size,
                    "note": "No products found for this store"
                }
            }
            
            # Create filename for empty file
            filename = f"{filename_prefix}-products"
            
            # Save empty file to Data Lake
            save_result = save_to_datalake(empty_data, datalake_key, data_lake_path, filename)
            
            if save_result:
                response_data = {
                    "status": "success",
                    "message": "No products found for this store - empty file created",
                    "records_count": 0,
                    "filename": f"{filename}.json",
                    "path": data_lake_path,
                    "note": "Empty file created to indicate endpoint was successfully checked"
                }
            else:
                response_data = {
                    "status": "error",
                    "message": "No products found and failed to create empty file",
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
        # Simple filename format to match Magento/BigCommerce pattern (no date)
        filename = f"{filename_prefix}-products"

        save_result = save_to_datalake(product_data, datalake_key, data_lake_path, filename)

        if save_result:
            response_data = {
                "status": "success",
                "message": "Successfully downloaded and saved products data",
                "records_count": len(items_list),
                "filename": f"{filename}.json",
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


def fetch_shopify_products(auth_token: str, graphql_url: str, page_size: str) -> dict:
    """
    Fetch Shopify products using GraphQL API with pagination
    """
    try:
        logging.info(f"Starting Shopify GraphQL product fetch from: {graphql_url}")
        
        # Headers for Shopify GraphQL API
        headers = {
            'X-Shopify-Access-Token': auth_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Scale nested query sizes based on main page_size for better performance
        main_page_size = int(page_size)
        collections_limit = min(10, max(5, main_page_size // 25))  # 5-10 collections
        variants_limit = min(50, max(10, main_page_size // 5))     # 10-50 variants  
        media_limit = min(25, max(5, main_page_size // 10))        # 5-25 media items
        
        logging.info(f"Scaled limits - Products: {main_page_size}, Collections: {collections_limit}, Variants: {variants_limit}, Media: {media_limit}")

        # GraphQL query for products with scaled nested limits
        graphql_query = {
            "query": f"""query {{
                products(first: {page_size}) {{
                    edges {{
                        node {{
                            id
                            title
                            category {{
                                name
                                fullName
                            }}
                            collections(first: {collections_limit}) {{
                                edges {{
                                    node {{
                                        title
                                    }}
                                }}
                            }}
                            vendor
                            productType
                            totalInventory
                            createdAt
                            handle
                            updatedAt
                            publishedAt
                            tags
                            status
                            variants(first: {variants_limit}) {{
                                edges {{
                                    node {{
                                        id
                                        title
                                        sku
                                        displayName
                                        price
                                        position
                                        compareAtPrice
                                        selectedOptions {{
                                            name
                                            value
                                        }}
                                        createdAt
                                        updatedAt
                                        taxable
                                        barcode
                                        inventoryQuantity
                                        product {{
                                            id
                                        }}
                                        image {{
                                            id
                                            altText
                                            url
                                            width
                                            height
                                        }}
                                    }}
                                }}
                            }}
                            options {{
                                id
                                name
                                position
                                values
                            }}
                            media(first: {media_limit}) {{
                                edges {{
                                    node {{
                                        id
                                        preview {{
                                            image {{
                                                url
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                    pageInfo {{
                        hasPreviousPage
                        hasNextPage
                        startCursor
                        endCursor
                    }}
                }}
            }}"""
        }

        all_products = []
        has_next_page = True
        cursor = None
        page_count = 0
        max_pages = 50  # Safety limit

        while has_next_page and page_count < max_pages:
            page_count += 1
            logging.info(f"Fetching page {page_count} of products...")

            # Add cursor for pagination if we have one
            if cursor:
                paginated_query = graphql_query["query"].replace(
                    f"products(first: {page_size})",
                    f"products(first: {page_size}, after: \"{cursor}\")"
                )
                current_query = {"query": paginated_query}
            else:
                current_query = graphql_query

            # Make GraphQL request
            response = requests.post(graphql_url, headers=headers, json=current_query, timeout=30)
            
            if response.status_code != 200:
                logging.error(f"Shopify GraphQL API error: {response.status_code}")
                logging.error(f"Response: {response.text}")
                return {
                    "error": "GRAPHQL_API_ERROR",
                    "message": f"Shopify GraphQL API returned status {response.status_code}",
                    "details": response.text
                }

            try:
                data = response.json()
                
                # Check for GraphQL errors
                if 'errors' in data:
                    logging.error(f"GraphQL errors: {data['errors']}")
                    return {
                        "error": "GRAPHQL_QUERY_ERROR",
                        "message": "GraphQL query returned errors",
                        "details": data['errors']
                    }

                # Extract products from GraphQL response
                products_data = data.get('data', {}).get('products', {})
                products = products_data.get('edges', [])
                page_info = products_data.get('pageInfo', {})

                # Add products to our collection
                for product_edge in products:
                    product = product_edge.get('node', {})
                    all_products.append(product)

                logging.info(f"Page {page_count}: Found {len(products)} products")

                # Check if there are more pages
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')

                if not has_next_page:
                    logging.info("No more pages to fetch")
                    break

            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON response: {e}")
                return {
                    "error": "JSON_PARSE_ERROR",
                    "message": f"Failed to parse Shopify GraphQL response: {str(e)}",
                    "details": response.text[:500]
                }

        logging.info(f"Completed fetching products. Total products: {len(all_products)}")

        # Return data in consistent format
        return {
            "data": all_products,
            "total_count": len(all_products),
            "metadata": {
                "source": graphql_url,
                "query_type": "GraphQL",
                "pages_fetched": page_count,
                "page_size": page_size,
                "has_more_pages": has_next_page
            }
        }

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error during Shopify GraphQL request: {str(e)}")
        import traceback
        return {
            "error": "FETCH_ERROR",
            "message": str(e),
            "traceback": traceback.format_exc()
        }
    except Exception as e:
        logging.error(f"Unexpected error in fetch_shopify_products: {str(e)}")
        import traceback
        return {
            "error": "UNEXPECTED_FETCH_ERROR",
            "message": str(e),
            "traceback": traceback.format_exc()
        }


@app.route(route="get_order_data")
def get_order_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Shopify order data download function processed a request.')

    try:
        # Get parameters from request
        auth_token = req.params.get('auth_token')
        base_url = req.params.get('base_url')
        api_version = req.params.get('api_version', '2024-10')
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'Retail/Shopify/Orders')
        page_size = req.params.get('page_size', '100')
        order_number_raw = req.params.get('order_number')
        order_number = None
        if order_number_raw:
            # Clean the input order number to only keep digits
            numeric_order_number = re.sub(r'\D', '', order_number_raw)
            if numeric_order_number:
                # Use the numeric part of the order number directly for the search
                order_number = numeric_order_number
            else:
                # If the order_number param contains no digits, it's invalid.
                return func.HttpResponse(
                    json.dumps({"status": "error", "message": "Invalid order_number parameter: must contain digits."}),
                    status_code=400, mimetype="application/json"
                )

        created_at_min = req.params.get('created_at_min')
        created_at_max = req.params.get('created_at_max')
        updated_at_min = req.params.get('updated_at_min')
        updated_at_max = req.params.get('updated_at_max')

        # Validate required parameters
        if not all([auth_token, base_url, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token, base_url, and datalake_key are required"}),
                status_code=400, mimetype="application/json"
            )

        # Construct full Shopify GraphQL URL
        full_base_url = f"https://{base_url}.myshopify.com/admin/api/{api_version}/graphql.json"
        
        logging.info(f"Fetching Shopify order data from: {full_base_url}")

        # Fetch Shopify order data
        order_data = fetch_shopify_orders(auth_token, full_base_url, page_size, order_number, created_at_min, created_at_max, updated_at_min, updated_at_max)

        if 'error' in order_data:
            return func.HttpResponse(json.dumps(order_data), status_code=500, mimetype="application/json")

        orders = order_data.get('data', [])
        if not orders:
            return func.HttpResponse(json.dumps({"status": "success", "message": "No new orders found.", "records_count": 0}), status_code=200, mimetype="application/json")

        # Save each order to Data Lake
        saved_files = []
        failed_files = []
        for raw_order in orders:
            # Transform the order to flatten nested connections like 'edges' and 'node'
            order = _transform_order(raw_order)
            if not order:
                logging.warning("Skipping an order that failed transformation.")
                continue


            # New filename logic: clean the order name to use for the filename.
            order_name = order.get('name')
            if order_name:
                # Remove any character that is not a letter, number, or dash.
                cleaned_name = re.sub(r'[^a-zA-Z0-9-]', '', order_name)
                # Remove any leading dashes.
                filename = cleaned_name.lstrip('-')
                
                if filename:
                    if save_order_to_datalake(order, datalake_key, data_lake_path, filename):
                        saved_files.append(f"{filename}.json")
                    else:
                        failed_files.append(f"{filename}.json")
                else:
                    # Fallback if the cleaned name is empty.
                    fallback_id = order.get('legacyResourceId', order.get('id', 'unknown_id'))
                    logging.warning(f"Could not generate a valid filename from order name: '{order_name}'. Fallback ID: {fallback_id}")
                    failed_files.append(f"FAILED_INVALID_NAME(id_{fallback_id})")
            else:
                # Fallback if the order has no 'name' field.
                fallback_id = order.get('legacyResourceId', order.get('id', 'unknown_id'))
                logging.warning(f"Order is missing 'name' field. Cannot save file. Fallback ID: {fallback_id}")
                failed_files.append(f"FAILED_NO_NAME(id_{fallback_id})")

        response_data = {
            "status": "partial_success" if failed_files else "success",
            "message": f"Processed {len(orders)} orders.",
            "records_saved": len(saved_files),
            "records_failed": len(failed_files),
            "saved_files": saved_files,
            "failed_files": failed_files,
            "path": data_lake_path
        }
        
        return func.HttpResponse(json.dumps(response_data), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Unexpected error in get_order_data: {str(e)}")
        import traceback
        return func.HttpResponse(
            json.dumps({"error": "UNEXPECTED_ERROR", "message": str(e), "traceback": traceback.format_exc()}),
            status_code=500, mimetype="application/json"
        )

def fetch_shopify_orders(auth_token: str, graphql_url: str, page_size: str, order_number: str = None, created_at_min: str = None, created_at_max: str = None, updated_at_min: str = None, updated_at_max: str = None) -> dict:
    headers = {'X-Shopify-Access-Token': auth_token, 'Content-Type': 'application/json'}
    
    # Build the filter query string
    if order_number:
        # If an order number is provided, perform a 'contains' search using wildcards
        query_filter = f"name:*{order_number}*"
    else:
        filters = []
        if created_at_min:
            filters.append(f"created_at:>= '{created_at_min}'")
        if created_at_max:
            filters.append(f"created_at:<= '{created_at_max}'")
        if updated_at_min:
            filters.append(f"updated_at:>= '{updated_at_min}'")
        if updated_at_max:
            filters.append(f"updated_at:<= '{updated_at_max}'")
        query_filter = " AND ".join(filters)
    logging.info(f"Using query filter: {query_filter}")

    # The GraphQL query structure is based on the 1004.json file provided.
    # This is a comprehensive query to get all relevant order details.
    query_template = """query($cursor: String, $query: String) {{
        orders(first: {page_size}, after: $cursor, query: $query, sortKey: UPDATED_AT, reverse: true) {{
            edges {{
                node {{
                    id
                    legacyResourceId
                    name
                    email
                    confirmed
                    processedAt
                    createdAt
                    updatedAt
                    closedAt
                    cancelReason
                    cancelledAt
                    customerLocale
                    currencyCode
                    phone
                    note
                    tags
                    taxExempt
                    estimatedTaxes
                    displayFinancialStatus
                    displayFulfillmentStatus
                    currentSubtotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                    currentTotalDiscountsSet {{ shopMoney {{ amount currencyCode }} }}
                    currentTotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                    currentTotalTaxSet {{ shopMoney {{ amount currencyCode }} }}
                    subtotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                    totalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                    totalShippingPriceSet {{ shopMoney {{ amount currencyCode }} }}
                    totalTaxSet {{ shopMoney {{ amount currencyCode }} }}
                    taxLines {{ title priceSet {{ shopMoney {{ amount currencyCode }} }} }}
                    shippingLines(first: 5) {{
                        edges {{
                            node {{
                                title
                                carrierIdentifier
                                price
                                source
                                originalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                                discountedPriceSet {{ shopMoney {{ amount currencyCode }} }}
                                taxLines {{ title priceSet {{ shopMoney {{ amount currencyCode }} }} }}
                            }}
                        }}
                    }}
                    customer {{ id email firstName lastName phone createdAt updatedAt defaultAddress {{ firstName lastName address1 address2 city province country zip phone countryCode provinceCode }} }}
                    shippingAddress {{ firstName lastName address1 address2 city province country zip phone company countryCode provinceCode name }}
                    billingAddress {{ firstName lastName address1 address2 city province country zip phone company countryCode provinceCode name }}
                    lineItems(first: 250) {{
                        edges {{
                            node {{
                                id
                                title
                                quantity
                                name
                                sku
                                variantTitle
                                vendor
                                fulfillableQuantity
                                fulfillmentStatus
                                taxable
                                requiresShipping
                                variant {{ id title price inventoryPolicy product {{ id title vendor productType }} }}
                                originalUnitPriceSet {{ shopMoney {{ amount currencyCode }} }}
                                discountedUnitPriceSet {{ shopMoney {{ amount currencyCode }} }}
                                originalTotalSet {{ shopMoney {{ amount currencyCode }} }}
                                discountedTotalSet {{ shopMoney {{ amount currencyCode }} }}
                                totalDiscountSet {{ shopMoney {{ amount currencyCode }} }}
                                taxLines {{ title priceSet {{ shopMoney {{ amount currencyCode }} }} }}
                            }}
                        }}
                    }}
                    fulfillments(first: 50) {{
                        id
                        status
                        createdAt
                        updatedAt
                        displayStatus
                        legacyResourceId
                        totalQuantity
                        trackingInfo(first: 50) {{
                            company
                            number
                            url
                        }}
                        fulfillmentLineItems(first: 50) {{
                            edges {{
                                node {{
                                    lineItem {{ id title quantity sku vendor variantTitle originalUnitPriceSet {{ shopMoney {{ amount currencyCode }} }} }}
                                    quantity
                                    
                                }}
                            }}
                        }}
                    }}
                    refunds(first: 50) {{
                        id
                        createdAt
                        note
                        totalRefundedSet {{ shopMoney {{ amount currencyCode }} }}
                        refundLineItems(first: 50) {{
                            edges {{
                                node {{
                                    lineItem {{ id title sku }}
                                    quantity
                                    subtotalSet {{ shopMoney {{ amount currencyCode }} }}
                                }}
                            }}
                        }}
                    }}
                    customAttributes {{ key value }}
                }}
            }}
            pageInfo {{
                hasNextPage
                endCursor
            }}
        }}
    }}"""

    all_orders = []
    cursor = None
    page_count = 0
    max_pages = 100 # Safety break

    while page_count < max_pages:
        page_count += 1
        variables = {"cursor": cursor, "query": query_filter if query_filter else None}
        graphql_query = {"query": query_template.format(page_size=page_size), "variables": variables}
        
        try:
            response = requests.post(graphql_url, headers=headers, json=graphql_query, timeout=60)
            response.raise_for_status()
            data = response.json()

            if 'errors' in data:
                logging.error(f"GraphQL errors: {data['errors']}")
                return {"error": "GRAPHQL_QUERY_ERROR", "details": data['errors']}

            orders_data = data.get('data', {}).get('orders', {})
            page_info = orders_data.get('pageInfo', {})

            for edge in orders_data.get('edges', []):
                all_orders.append(edge['node'])

            logging.info(f"Page {page_count}: Fetched {len(orders_data.get('edges', []))} orders. Total so far: {len(all_orders)}")

            if not page_info.get('hasNextPage'):
                break
            cursor = page_info.get('endCursor')

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error fetching Shopify orders: {e}")
            return {"error": "FETCH_ERROR", "message": str(e)}

    return {"data": all_orders, "total_count": len(all_orders)}

def save_order_to_datalake(data: dict, datalake_key: str, path: str, filename: str) -> bool:
    try:
        account_name = "prodbimanager"
        account_url = f"https://{account_name}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=datalake_key)
        file_system_client = service_client.get_file_system_client("prodbidlstorage")
        
        # Ensure .json extension
        if not filename.endswith('.json'):
            filename = f"{filename}.json"

        file_path = f"{path}/{filename}"
        json_data = json.dumps(data, indent=4, default=str)
        
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(json_data, overwrite=True)
        
        logging.info(f"Successfully saved order to Data Lake: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving order {filename} to Data Lake: {e}")
        return False

def save_to_datalake(data: dict, datalake_key: str, path: str, filename: str = None) -> bool:
    """
    Save data to Azure Data Lake Storage
    """
    try:
        logging.info("Starting Data Lake save operation...")
        
        # Data Lake configuration (matching BigCommerce/Magento)
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
            filename = f"shopify_products.{timestamp}"
        
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

def _flatten_connection(connection_dict):
    """Helper function to transform a GraphQL connection ({'edges': [{'node': ...}]}) into a simple list."""
    if not connection_dict or 'edges' not in connection_dict or not isinstance(connection_dict['edges'], list):
        return []
    return [edge['node'] for edge in connection_dict['edges'] if 'node' in edge]

def _transform_order(order):
    """Transforms a raw order from GraphQL to flatten nested connections."""
    if not order:
        return None

    # Flatten lineItems and process new fields
    if 'lineItems' in order and order['lineItems']:
        line_items = _flatten_connection(order['lineItems'])
        for item in line_items:
            # Combine 'name' and 'title' for a comprehensive line item name
            item['line_item_name'] = item.get('name', item.get('title', ''))
            # Capture the fulfillment status for the line item
            item['line_item_fulfillment_status'] = item.get('fulfillmentStatus')
        order['lineItems'] = line_items

    # Flatten fulfillments and their nested items
    if 'fulfillments' in order and order['fulfillments']:
        for fulfillment in order['fulfillments']:
            if 'fulfillmentLineItems' in fulfillment and fulfillment['fulfillmentLineItems']:
                fulfillment['fulfillmentLineItems'] = _flatten_connection(fulfillment['fulfillmentLineItems'])

    # Flatten refunds and their nested items
    if 'refunds' in order and order['refunds']:
        for refund in order['refunds']:
            if 'refundLineItems' in refund and refund['refundLineItems']:
                refund['refundLineItems'] = _flatten_connection(refund['refundLineItems'])

    return order


@app.route(route="get_status_data")
def get_status_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Shopify status data download function processed a request.')

    try:
        # Get parameters from request
        auth_token = req.params.get('auth_token')
        base_url = req.params.get('base_url')
        api_version = req.params.get('api_version', '2024-10')
        datalake_key = req.params.get('datalake_key')
        data_lake_path = req.params.get('data_lake_path', 'Retail/Shopify/OrderStatus')
        page_size = req.params.get('page_size', '100')
        order_number_raw = req.params.get('order_number')
        order_number = None
        if order_number_raw:
            numeric_order_number = re.sub(r'\D', '', order_number_raw)
            if numeric_order_number:
                order_number = numeric_order_number
            else:
                return func.HttpResponse(
                    json.dumps({"status": "error", "message": "Invalid order_number parameter: must contain digits."}),
                    status_code=400, mimetype="application/json"
                )

        created_at_min = req.params.get('created_at_min')
        created_at_max = req.params.get('created_at_max')
        updated_at_min = req.params.get('updated_at_min')
        updated_at_max = req.params.get('updated_at_max')

        if not all([auth_token, base_url, datalake_key]):
            return func.HttpResponse(
                json.dumps({"error": "MISSING_PARAMETER", "message": "auth_token, base_url, and datalake_key are required"}),
                status_code=400, mimetype="application/json"
            )

        full_base_url = f"https://{base_url}.myshopify.com/admin/api/{api_version}/graphql.json"
        logging.info(f"Fetching Shopify order status data from: {full_base_url}")

        status_data = fetch_shopify_statuses(auth_token, full_base_url, page_size, order_number, created_at_min, created_at_max, updated_at_min, updated_at_max)

        if 'error' in status_data:
            return func.HttpResponse(json.dumps(status_data), status_code=500, mimetype="application/json")

        orders = status_data.get('data', [])
        if not orders:
            # Include debugging information in the response when no orders found
            debug_info = {
                "status": "success", 
                "message": "No new order statuses found.", 
                "records_count": 0,
                "debug_info": {
                    "created_at_min": created_at_min,
                    "created_at_max": created_at_max,
                    "updated_at_min": updated_at_min,
                    "updated_at_max": updated_at_max,
                    "order_number": order_number,
                    "query_filter_used": status_data.get('query_filter_used', 'Not available'),
                    "total_pages_checked": status_data.get('total_pages_checked', 'Not available')
                }
            }
            return func.HttpResponse(json.dumps(debug_info), status_code=200, mimetype="application/json")

        saved_files = []
        failed_files = []
        for raw_order in orders:
            order = _transform_order(raw_order)
            if not order:
                logging.warning("Skipping an order that failed transformation.")
                continue

            order_name = order.get('name')
            if order_name:
                cleaned_name = re.sub(r'[^a-zA-Z0-9-]', '', order_name)
                filename = cleaned_name.lstrip('-')
                
                if filename:
                    if save_order_to_datalake(order, datalake_key, data_lake_path, filename):
                        saved_files.append(f"{filename}.json")
                    else:
                        failed_files.append(f"{filename}.json")
                else:
                    fallback_id = order.get('legacyResourceId', order.get('id', 'unknown_id'))
                    logging.warning(f"Could not generate a valid filename from order name: '{order_name}'. Fallback ID: {fallback_id}")
                    failed_files.append(f"FAILED_INVALID_NAME(id_{fallback_id})")
            else:
                fallback_id = order.get('legacyResourceId', order.get('id', 'unknown_id'))
                logging.warning(f"Order is missing 'name' field. Cannot save file. Fallback ID: {fallback_id}")
                failed_files.append(f"FAILED_NO_NAME(id_{fallback_id})")

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

def fetch_shopify_statuses(auth_token: str, graphql_url: str, page_size: str, order_number: str = None, created_at_min: str = None, created_at_max: str = None, updated_at_min: str = None, updated_at_max: str = None) -> dict:
    headers = {'X-Shopify-Access-Token': auth_token, 'Content-Type': 'application/json'}
    
    if order_number:
        query_filter = f"name:*{order_number}*"
    else:
        filters = []
        if created_at_min:
            filters.append(f"created_at:>='{created_at_min}'")
        if created_at_max:
            filters.append(f"created_at:<='{created_at_max}'")
        if updated_at_min:
            filters.append(f"updated_at:>='{updated_at_min}'")
        if updated_at_max:
            filters.append(f"updated_at:<='{updated_at_max}'")
        query_filter = " AND ".join(filters)

    # ==> ADDED FOR DEBUGGING <==
    logging.info(f"Constructed Shopify Query Filter: '{query_filter}'")
    # ============================

    query_template = """query($cursor: String, $query: String) {{
        orders(first: {page_size}, after: $cursor, query: $query, sortKey: UPDATED_AT, reverse: true) {{
            edges {{
                node {{
                    id
                    legacyResourceId
                    name
                    displayFinancialStatus
                    displayFulfillmentStatus
                    updatedAt
                    fulfillments(first: 10) {{
                        id
                        status
                        displayStatus
                        createdAt
                        updatedAt
                        legacyResourceId
                        trackingInfo(first: 10) {{
                            company
                            number
                            url
                        }}
                    }}
                }}
            }}
            pageInfo {{
                hasNextPage
                endCursor
            }}
        }}
    }}"""

    all_orders = []
    cursor = None
    page_count = 0
    max_pages = 100

    while page_count < max_pages:
        page_count += 1
        variables = {"cursor": cursor, "query": query_filter if query_filter else None}
        graphql_query = {"query": query_template.format(page_size=page_size), "variables": variables}

        # ==> ADDED FOR DEBUGGING <==
        logging.info(f"Executing GraphQL Query (Page {page_count}): {json.dumps(graphql_query, indent=2)}")
        # ===========================
    else:
        filters = []
        if created_at_min:
            filters.append(f"created_at:>='{created_at_min}'")
        if created_at_max:
            filters.append(f"created_at:<='{created_at_max}'")
        if updated_at_min:
            filters.append(f"updated_at:>='{updated_at_min}'")
        if updated_at_max:
            filters.append(f"updated_at:<='{updated_at_max}'")
        query_filter = " AND ".join(filters)
    logging.info(f"Using query filter for statuses: {query_filter}")

    query_template = """query($cursor: String, $query: String) {{
        orders(first: {page_size}, after: $cursor, query: $query) {{
            edges {{
                node {{
                    id
                    name
                    displayFinancialStatus
                    displayFulfillmentStatus
                    lineItems(first: 100) {{
                        edges {{
                            node {{
                                id
                                name
                                fulfillmentStatus
                            }}
                        }}
                    }}
                    fulfillments(first: 100) {{
                        id
                        name
                        displayStatus
                        status
                        fulfillmentLineItems(first: 100) {{
                            edges {{
                                node {{
                                    id
                                    originalTotal
                                    lineItem {{ id }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            pageInfo {{
                hasNextPage
                endCursor
            }}
        }}
    }}"""

    all_orders = []
    cursor = None
    page_count = 0
    max_pages = 100

    while page_count < max_pages:
        page_count += 1
        variables = {"cursor": cursor, "query": query_filter if query_filter else None}
        graphql_query = {"query": query_template.format(page_size=page_size), "variables": variables}
        
        try:
            response = requests.post(graphql_url, headers=headers, json=graphql_query, timeout=60)
            response.raise_for_status()
            data = response.json()

            if 'errors' in data:
                logging.error(f"GraphQL errors: {data['errors']}")
                return {"error": "GRAPHQL_QUERY_ERROR", "details": data['errors']}

            orders_data = data.get('data', {}).get('orders', {})
            page_info = orders_data.get('pageInfo', {})

            for edge in orders_data.get('edges', []):
                all_orders.append(edge['node'])

            logging.info(f"Page {page_count}: Fetched {len(orders_data.get('edges', []))} order statuses. Total so far: {len(all_orders)}")

            if not page_info.get('hasNextPage'):
                break
            cursor = page_info.get('endCursor')

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error fetching Shopify order statuses: {e}")
            return {"error": "FETCH_ERROR", "message": str(e)}

    return {
        "data": all_orders, 
        "total_count": len(all_orders),
        "query_filter_used": query_filter,
        "total_pages_checked": page_count
    }
