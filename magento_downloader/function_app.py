import azure.functions as func
import json
import logging
import requests
import time
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

@app.route(route="get_magento_data", auth_level=func.AuthLevel.FUNCTION)
def get_magento_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Magento data request')
    
    try:
        # Get required parameters
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: auth_token"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: datalake_key"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get other parameters
        base_url = req.params.get('base_url', 'www.voltlighting.com')
        channel = req.params.get('channel', 'costco_next')
        api_version = req.params.get('api_version', 'V1')
        item = req.params.get('item', 'orders')
        start_date = req.params.get('start_date')
        end_date = req.params.get('end_date')
        data_lake_path = req.params.get('data_lake_path', f'SalesOrders/input/files/json/graphql/{item}')
        filename_prefix = req.params.get('filename', 'data')
        store_id = req.params.get('store_id')  # Store ID parameter
        filter_type = req.params.get('filter_type', 'from_to')  # 'from_to' or 'gteq_lteq'
        
        # Fixed page size
        page_size = 50
        
        # Validate required parameters
        if not start_date or not end_date:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameters: start_date and end_date"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validate item type
        if item not in ['orders', 'shipments']:
            return func.HttpResponse(
                json.dumps({"error": "Item must be 'orders' or 'shipments'"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validate filter_type
        if filter_type not in ['from_to', 'gteq_lteq']:
            return func.HttpResponse(
                json.dumps({"error": "filter_type must be 'from_to' or 'gteq_lteq'"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Add Bearer prefix if needed
        if not auth_token.startswith('Bearer '):
            auth_token = f'Bearer {auth_token}'
        
        # Build base URL
        if not base_url.startswith('http'):
            base_url = f'https://{base_url}'
        
        url = f"{base_url}/rest/{channel}/{api_version}/{item}/"
        
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Process each day in the date range
        current_date = start
        total_items_all_days = 0
        total_files_saved = []
        all_errors = []
        
        logging.info(f"Processing date range from {start_date} to {end_date}")
        logging.info(f"Store ID: {store_id}, Filter type: {filter_type}")
        
        while current_date <= end:
            day_str = current_date.strftime('%Y-%m-%d')
            logging.info(f"Processing {day_str}")
            
            # Use a set to store unique item IDs to avoid duplicates
            day_items_dict = {}
            
            # Fetch items CREATED on this day
            created_items = fetch_items_for_date(url, auth_token, day_str, 'created_at', item, page_size, store_id, filter_type)
            for item_data in created_items:
                item_id = item_data.get('entity_id') or item_data.get('increment_id')
                if item_id:
                    day_items_dict[item_id] = item_data
            
            logging.info(f"{day_str}: Found {len(day_items_dict)} items created")
            
            # Fetch items UPDATED on this day
            updated_items = fetch_items_for_date(url, auth_token, day_str, 'updated_at', item, page_size, store_id, filter_type)
            for item_data in updated_items:
                item_id = item_data.get('entity_id') or item_data.get('increment_id')
                if item_id:
                    # This will overwrite if already exists (no duplicates)
                    day_items_dict[item_id] = item_data
            
            logging.info(f"{day_str}: Total {len(day_items_dict)} unique items (created or updated)")
            
            # Convert dict back to list
            day_items = list(day_items_dict.values())
            
            # Enhance order items with variant IDs if processing orders
            if item == 'orders' and day_items:
                day_items = enhance_order_items_with_variant_ids(day_items)
            
            # Save this day's data if we have items
            if day_items:
                logging.info(f"Saving {len(day_items)} items for {day_str}")
                
                # Generate filename for this day: filename.YYYYMMDD-item.json
                day_filename = f"{filename_prefix}.{current_date.strftime('%Y%m%d')}-{item}"
                
                # Prepare the data structure for this day
                day_data = {
                    "total_count": len(day_items),
                    "items": day_items,
                    "metadata": {
                        "source": base_url,
                        "channel": channel,
                        "item_type": item,
                        "date": day_str,
                        "date_field_used": "created_at OR updated_at",
                        "store_id": store_id,
                        "filter_type": filter_type,
                        "fetch_timestamp": datetime.now().isoformat()
                    }
                }
                
                save_result = save_to_datalake(
                    data=day_data,
                    datalake_key=datalake_key,
                    path=data_lake_path,
                    filename=day_filename,
                    page=None
                )
                
                if save_result.get('success'):
                    total_files_saved.append({
                        "date": day_str,
                        "path": save_result.get('path'),
                        "items_count": len(day_items),
                        "size_bytes": save_result.get('size_bytes')
                    })
                    total_items_all_days += len(day_items)
                else:
                    all_errors.append(f"Failed to save {day_str}: {save_result.get('error')}")
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # Small delay between days
            time.sleep(0.5)
        
        # Prepare final result
        result = {
            "success": len(all_errors) == 0,
            "message": f"Processed {(end - start).days + 1} days, saved {len(total_files_saved)} files (created OR updated)",
            "store_id_filter": store_id,
            "filter_type_used": filter_type,
            "total_items_all_days": total_items_all_days,
            "files_saved": total_files_saved,
            "days_processed": (end - start).days + 1,
            "errors": all_errors if all_errors else None
        }
        
        return func.HttpResponse(
            json.dumps(result, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="get_magento_products", auth_level=func.AuthLevel.FUNCTION)
def get_magento_products(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Magento products request')
    
    try:
        # Get required parameters
        auth_token = req.params.get('auth_token')
        datalake_key = req.params.get('datalake_key')
        
        if not auth_token:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: auth_token"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not datalake_key:
            return func.HttpResponse(
                json.dumps({"error": "Missing required parameter: datalake_key"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get other parameters
        base_url = req.params.get('base_url', 'www.voltlighting.com')
        channel = req.params.get('channel', 'costco_next')
        api_version = req.params.get('api_version', 'V1')
        item = req.params.get('item', 'products')  # products, categories, stockitems
        data_lake_path = req.params.get('data_lake_path', f'RetailProducts/input/files/json/graphql/{item}')
        filename_prefix = req.params.get('filename', item)
        store_id = req.params.get('store_id')  # Store ID parameter (optional)
        
        # Fixed page size
        page_size = 50
        
        # Validate item type
        if item not in ['products', 'categories', 'stockitems', 'stockItems']:
            return func.HttpResponse(
                json.dumps({"error": "Item must be 'products', 'categories', 'stockitems', or 'stockItems'"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Add Bearer prefix if needed
        if not auth_token.startswith('Bearer '):
            auth_token = f'Bearer {auth_token}'
        
        # Build base URL
        if not base_url.startswith('http'):
            base_url = f'https://{base_url}'
        
        # Build endpoint URL based on item type
        if item == 'categories':
            # Use the correct categories/list endpoint
            url = f"{base_url}/rest/{channel}/{api_version}/categories/list"
        elif item in ['stockitems', 'stockItems']:
            url = f"{base_url}/rest/{channel}/{api_version}/stockItems/lowStock"
        else:
            url = f"{base_url}/rest/{channel}/{api_version}/{item}"
        
        logging.info(f"Processing all {item}")
        logging.info(f"Store ID filter: {store_id}")
        
        # Fetch all items
        all_items = fetch_all_items(url, auth_token, page_size, store_id, item)
        
        # Post-process categories if store_id filter was requested
        if item == 'categories' and store_id and all_items:
            # Categories might not support store_id filtering at API level
            # So we filter after fetching all categories
            original_count = len(all_items)
            # For now, we'll keep all categories since we don't know the exact filtering logic
            # This ensures we get the categories data for analysis
            logging.info(f"Categories fetched: {original_count} total categories (store_id filtering may not apply to categories)")
        
        total_items = len(all_items)
        all_errors = []
        
        # Save items data if we have any
        if all_items:
            logging.info(f"Saving {total_items} {item}")
            
            # Generate filename: filename-item.json
            filename = f"{filename_prefix}-{item}"
            
            # Prepare the data structure
            items_data = {
                "total_count": total_items,
                "items": all_items,
                "metadata": {
                    "source": base_url,
                    "channel": channel,
                    "item_type": item,
                    "store_id": store_id,
                    "fetch_timestamp": datetime.now().isoformat()
                }
            }
            
            save_result = save_to_datalake(
                data=items_data,
                datalake_key=datalake_key,
                path=data_lake_path,
                filename=filename,
                page=None
            )
            
            if not save_result.get('success'):
                all_errors.append(f"Failed to save {item}: {save_result.get('error')}")
        
        # Prepare final result
        result = {
            "success": len(all_errors) == 0,
            "message": f"Processed all {item}, found {total_items} items",
            "item_type": item,
            "store_id_filter": store_id,
            "total_items": total_items,
            "file_saved": save_result.get('path') if all_items and save_result.get('success') else None,
            "file_size_bytes": save_result.get('size_bytes') if all_items and save_result.get('success') else None,
            "errors": all_errors if all_errors else None
        }
        
        return func.HttpResponse(
            json.dumps(result, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def fetch_all_items(url: str, auth_token: str, page_size: int, store_id: str = None, item_type: str = 'products') -> list:
    """Fetch all items (products, categories, stockitems), optionally filtered by store_id."""
    all_items = []
    current_page = 1
    
    while True:
        # Build query parameters based on item type
        if item_type in ['stockitems', 'stockItems']:
            # Special parameters for stockitems endpoint
            params = {
                'scopeId': '0',
                'qty': '1000',  # Reduced from 999999 for better performance
                'pageSize': page_size,
                'currentPage': current_page,
                'fields': 'items[item_id,product_id,qty]'
            }
        elif item_type == 'categories':
            # Categories API has known pagination issues - use single request approach
            # Based on Magento documentation, currentPage doesn't work reliably for categories
            if current_page > 1:
                # Categories typically return all data in first request, so skip subsequent pages
                logging.info(f"Categories API: Skipping page {current_page} - categories typically return all data in first request")
                break
            
            params = {
                'searchCriteria[pageSize]': 5000,  # Use very large page size to get all categories at once
                'fields': 'items[id,parent_id,name,position]'
                # Note: Omitting currentPage as it doesn't work reliably for categories
            }
            # Note: Categories may not support store_id filtering like products do
        else:
            # Standard parameters for products
            if store_id:
                params = {
                    'searchCriteria[filter_groups][0][filters][0][field]': 'store_id',
                    'searchCriteria[filter_groups][0][filters][0][value]': store_id,
                    'searchCriteria[filter_groups][0][filters][0][condition_type]': 'eq',
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
            else:
                params = {
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
        
        # Make request
        headers = {
            'Authorization': auth_token,
            'Content-Type': 'application/json'
        }
        
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{url}?{query_string}"
        
        logging.info(f"Calling {item_type} URL: {full_url}")
        logging.info(f"Parameters: {params}")
        
        try:
            response = requests.get(full_url, headers=headers, timeout=30)
            logging.info(f"Response status code: {response.status_code}")
            response.raise_for_status()
            
            data = response.json()
            logging.info(f"Raw response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            logging.info(f"Full response data (first 500 chars): {str(data)[:500]}")
            
            # Handle response structures - all item types support pagination
            items = data.get("items", [])
            total_count = data.get("total_count", 0)
            
            logging.info(f"Response received: {len(items)} {item_type} found on page {current_page}, total_count: {total_count}")
            
            # Special debugging for categories with store_id filter
            if item_type == 'categories' and not items and current_page == 1:
                logging.warning(f"No categories found on first page with current filters. This could mean:")
                logging.warning(f"1. No categories exist for the specified store_id filter")
                logging.warning(f"2. Categories don't use store_id filtering the same way as products")
                logging.warning(f"3. The store_id value might not match any categories")
                logging.warning(f"Consider testing without store_id filter to verify categories exist")
            
            if not items:
                logging.info(f"No items found on page {current_page}, stopping pagination")
                break
            
            all_items.extend(items)
            current_page += 1
            
            # Check if we've reached the end based on page size
            expected_page_size = 250 if item_type == 'categories' else page_size
            if len(items) < expected_page_size:
                logging.info(f"Received fewer items than expected page size ({len(items)} < {expected_page_size}), assuming last page")
                break
            
            # Small delay to avoid rate limiting
            time.sleep(0.3)
            
        except requests.exceptions.Timeout:
            logging.error(f"Timeout calling {url} for page {current_page}")
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {item_type} page {current_page}: {str(e)}")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            break
    
    logging.info(f"Total {item_type} fetched: {len(all_items)}")
    return all_items


def fetch_items_for_date(url: str, auth_token: str, day_str: str, date_field: str, item_type: str, page_size: int, store_id: str = None, filter_type: str = 'from_to') -> list:
    """Fetch all items for a specific date and field."""
    all_items = []
    current_page = 1
    
    while True:
        # Build query parameters based on filter_type
        if filter_type == 'gteq_lteq':
            # EZ-UP style: separate filter groups for each condition
            if store_id:
                params = {
                    'searchCriteria[filter_groups][0][filters][0][field]': 'store_id',
                    'searchCriteria[filter_groups][0][filters][0][value]': store_id,
                    'searchCriteria[filter_groups][0][filters][0][condition_type]': 'eq',
                    'searchCriteria[filter_groups][1][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][1][filters][0][value]': f'{day_str}',
                    'searchCriteria[filter_groups][1][filters][0][condition_type]': 'gteq',
                    'searchCriteria[filter_groups][2][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][2][filters][0][value]': f'{day_str} 23:59:59',
                    'searchCriteria[filter_groups][2][filters][0][condition_type]': 'lteq',
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
            else:
                params = {
                    'searchCriteria[filter_groups][0][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][0][filters][0][value]': f'{day_str}',
                    'searchCriteria[filter_groups][0][filters][0][condition_type]': 'gteq',
                    'searchCriteria[filter_groups][1][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][1][filters][0][value]': f'{day_str} 23:59:59',
                    'searchCriteria[filter_groups][1][filters][0][condition_type]': 'lteq',
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
        else:
            # Volt style: from/to in same filter group
            if store_id:
                params = {
                    'searchCriteria[filter_groups][0][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][0][filters][0][value]': f'{day_str} 00:00:00',
                    'searchCriteria[filter_groups][0][filters][0][condition_type]': 'from',
                    'searchCriteria[filter_groups][0][filters][1][field]': date_field,
                    'searchCriteria[filter_groups][0][filters][1][value]': f'{day_str} 23:59:59',
                    'searchCriteria[filter_groups][0][filters][1][condition_type]': 'to',
                    'searchCriteria[filter_groups][1][filters][0][field]': 'store_id',
                    'searchCriteria[filter_groups][1][filters][0][value]': store_id,
                    'searchCriteria[filter_groups][1][filters][0][condition_type]': 'eq',
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
            else:
                params = {
                    'searchCriteria[filter_groups][0][filters][0][field]': date_field,
                    'searchCriteria[filter_groups][0][filters][0][value]': f'{day_str} 00:00:00',
                    'searchCriteria[filter_groups][0][filters][0][condition_type]': 'from',
                    'searchCriteria[filter_groups][0][filters][1][field]': date_field,
                    'searchCriteria[filter_groups][0][filters][1][value]': f'{day_str} 23:59:59',
                    'searchCriteria[filter_groups][0][filters][1][condition_type]': 'to',
                    'searchCriteria[pageSize]': page_size,
                    'searchCriteria[currentPage]': current_page
                }
        
        # Make request
        headers = {
            'Authorization': auth_token,
            'Content-Type': 'application/json'
        }
        
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{url}?{query_string}"
        
        logging.info(f"Calling URL with {filter_type}: {full_url[:200]}...")
        
        try:
            response = requests.get(full_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            items = data.get("items", [])
            
            logging.info(f"Response received: {len(items)} items found for {date_field} on {day_str}")
            
            if not items:
                break
            
            all_items.extend(items)
            current_page += 1
            
            # Small delay to avoid rate limiting
            time.sleep(0.3)
            
        except requests.exceptions.Timeout:
            logging.error(f"Timeout calling {url} for {day_str} page {current_page}")
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {day_str} {date_field} page {current_page}: {str(e)}")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            break
    
    return all_items


def save_to_datalake(data: dict, datalake_key: str, path: str, filename: str = None, page: int = None) -> dict:
    """Save data to Azure Data Lake Storage."""
    try:
        # Initialize Data Lake client
        account_name = "prodbimanager"
        account_url = f"https://{account_name}.dfs.core.windows.net"
        
        service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=datalake_key
        )
        
        # Get filesystem client
        filesystem_name = "prodbidlstorage"
        filesystem_client = service_client.get_file_system_client(filesystem_name)
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}"
        
        # Add page number only if provided
        if page:
            filename = f"{filename}_page{page}"
        
        # Ensure .json extension
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        # Full file path
        file_path = f"{path.strip('/')}/{filename}"
        
        # Create file and upload
        file_client = filesystem_client.get_file_client(file_path)
        json_data = json.dumps(data, default=str, indent=2)
        file_client.upload_data(json_data, overwrite=True)
        
        logging.info(f"Successfully saved to Data Lake: {file_path}")
        
        return {
            "success": True,
            "path": file_path,
            "filesystem": filesystem_name,
            "size_bytes": len(json_data)
        }
        
    except Exception as e:
        logging.error(f"Data Lake save error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def enhance_order_items_with_variant_ids(orders: list) -> list:
    """
    Enhance order line items with variant IDs for easier product joining.
    For Magento: variant_id = child product_id for configurable products, or product_id for simple products.
    """
    enhanced_orders = []
    
    for order in orders:
        enhanced_order = order.copy()
        order_items = enhanced_order.get('items', [])
        
        if order_items:
            enhanced_items = []
            
            for item in order_items:
                enhanced_item = item.copy()
                
                # Determine variant ID based on Magento product structure
                product_id = item.get('product_id')
                parent_item_id = item.get('parent_item_id')
                product_type = item.get('product_type', '')
                
                # Logic for variant ID:
                # - If it has a parent_item_id, it's a child of configurable -> use its product_id as variant_id
                # - If it's a simple product (no parent), use its product_id as variant_id  
                # - If it's a configurable parent, variant_id will be null (use children instead)
                
                if parent_item_id:
                    # This is a child item of a configurable product
                    variant_id = product_id
                    item_type = 'configurable_child'
                elif product_type == 'simple':
                    # This is a standalone simple product
                    variant_id = product_id
                    item_type = 'simple'
                elif product_type == 'configurable':
                    # This is a configurable parent - don't use for variant matching
                    variant_id = None
                    item_type = 'configurable_parent'
                else:
                    # Other product types (bundle, etc.)
                    variant_id = product_id
                    item_type = product_type or 'unknown'
                
                # Insert variant_id right after product_id for better positioning
                # Create a new ordered dict to control field order
                ordered_item = {}
                
                # Add fields in desired order - variant_id right after product_id
                for key, value in enhanced_item.items():
                    ordered_item[key] = value
                    if key == 'product_id':
                        ordered_item['variant_id'] = variant_id
                
                # Add classification fields at the end if not already present
                if 'variant_id' not in ordered_item:
                    ordered_item['variant_id'] = variant_id
                
                ordered_item['item_type'] = item_type
                ordered_item['is_variant'] = variant_id is not None
                ordered_item['is_configurable_child'] = parent_item_id is not None
                
                enhanced_item = ordered_item
                
                enhanced_items.append(enhanced_item)
            
            enhanced_order['items'] = enhanced_items
            
            # Add summary info to the order level
            total_items = len(enhanced_items)
            variant_items = len([item for item in enhanced_items if item.get('variant_id')])
            
            enhanced_order['items_summary'] = {
                'total_items': total_items,
                'variant_items': variant_items,
                'configurable_parents': len([item for item in enhanced_items if item.get('item_type') == 'configurable_parent']),
                'configurable_children': len([item for item in enhanced_items if item.get('item_type') == 'configurable_child']),
                'simple_products': len([item for item in enhanced_items if item.get('item_type') == 'simple'])
            }
        
        enhanced_orders.append(enhanced_order)
    
    return enhanced_orders