import azure.functions as func
import json
import logging
import requests
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
