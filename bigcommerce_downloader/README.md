# BigCommerce Catalog Data Downloader

Azure Function App for downloading product catalog data from BigCommerce using the REST API.

## Function Details

- **Function App Name**: `bigcommerce-downloader`
- **Function Name**: `get_product_data`
- **Runtime**: Python 3.11
- **Trigger**: HTTP

## Supported BigCommerce Catalog Endpoints

- **Products**: `/v3/catalog/products` (default)
- **Brands**: `/v3/catalog/brands`
- **Categories**: `/v3/catalog/categories`
- **Category Trees**: `/v3/catalog/trees`
- **Product Variants**: `/v3/catalog/products/{product_id}/variants`
- **Product Options**: `/v3/catalog/products/{product_id}/options`
- **Product Images**: `/v3/catalog/products/{product_id}/images`

## Parameters

### Required Parameters
- `auth_token`: BigCommerce API token (X-Auth-Token header)
- `datalake_key`: Azure Data Lake Storage access key

### Optional Parameters
- `base_url`: BigCommerce store hash only (e.g., lb2chb77ok) - function adds api.bigcommerce.com/stores/ automatically
- `api_version`: API version (default: v3)
- `item`: Catalog endpoint to fetch (default: products)
  - Options: products, brands, categories, trees, variants, options, images
- `page_size`: Number of items per page (default: 250, max: 250)
- `data_lake_path`: Azure Data Lake path (default: RetailProducts/input/files/json/products/base)
- `filename`: Filename prefix (default: bigcommerce)

## BigCommerce API Authentication

The function uses BigCommerce's REST API with X-Auth-Token authentication:

1. **API Token**: Obtained from BigCommerce store admin panel
2. **Store Hash**: Part of the base URL (e.g., lb2chb77ok)
3. **Headers**: 
   - `X-Auth-Token`: Your API token
   - `Content-Type`: application/json
   - `Accept`: application/json

## Example Usage

### Get Products
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=products&datalake_key={datalake_key}
```

### Get Brands
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=brands&datalake_key={datalake_key}
```

### Get Categories
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=categories&datalake_key={datalake_key}
```

### Get Category Trees
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=trees&datalake_key={datalake_key}
```

### Get Product Variants
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=variants&datalake_key={datalake_key}
```

### Get Product Options
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=options&datalake_key={datalake_key}
```

### Get Product Images
```
https://bigcommerce-downloader.azurewebsites.net/api/get_product_data?code={function_key}&auth_token={api_token}&base_url=lb2chb77ok&item=images&datalake_key={datalake_key}
```

## Data Lake Storage

- **Account**: prodbimanager
- **File System**: prodbidlstorage
- **Default Path**: RetailProducts/input/files/json/products/base
- **Filename Format**: `{filename_prefix}-{item}.json`

## Response Format

### Success Response
```json
{
  "status": "success",
  "message": "Successfully downloaded and saved products data",
  "records_count": 150,
  "filename": "bigcommerce.CXR045-products.json",
  "path": "RetailProducts/input/files/json/products/base"
}
```

### Error Response
```json
{
  "error": "API_CALL_FAILED",
  "status_code": 401,
  "response_text": "Unauthorized"
}
```

### No Data Response
```json
{
  "status": "success",
  "message": "No products found in the specified date range",
  "records_count": 0,
  "filename": null,
  "path": null,
  "note": "No file created - no products to save"
}
```

## File Creation Logic

- ✅ **Creates file**: When actual catalog data is found
- ❌ **No file created**: When API errors occur
- ❌ **No file created**: When zero records found

## Function 2: `get_order_data`

Downloads order-related data from the BigCommerce v2 REST API.

### Item Parameter

This function uses an `item` parameter to specify which data to download. Supported values are:
- **`Orders`**: Fetches all orders. (Endpoint: `/v2/orders`)
- **`OrderLine`**: Fetches all product line items across all orders. (Endpoint: `/v2/orders/products`)
- **`Fulfillments`**: Fetches all fulfillments (shipments). (Endpoint: `/v2/orders/shipments`)
- **`FulfillmentLines`**: Fetches all line items within each fulfillment.

### Required Parameters

- **`auth_token`**: BigCommerce API token.
- **`base_url`**: BigCommerce store hash (e.g., `lb2chb77ok`).
- **`datalake_key`**: Azure Data Lake Storage access key.

### Optional Parameters

- **`item`**: The type of order data to download (default: `Orders`).
- **`data_lake_path`**: Azure Data Lake path (default: `Retail/BigCommerce`).
- **`filename`**: Filename prefix (default: `bigcommerce`). The final filename will be `{filename}-{item}.json`.
- **`min_date_created`**: The minimum creation date for orders (format: `rfc2822` or `iso8601`).
- **`max_date_created`**: The maximum creation date for orders.

### Example Usage

#### Get All Orders Created After a Specific Date

```
https://bigcommerce-downloader.azurewebsites.net/api/get_order_data?code=YOUR_KEY&auth_token=YOUR_TOKEN&base_url=lb2chb77ok&datalake_key=YOUR_LAKE_KEY&item=Orders&min_date_created=2024-01-01T00:00:00Z
```

#### Get All Order Line Items

```
https://bigcommerce-downloader.azurewebsites.net/api/get_order_data?code=YOUR_KEY&auth_token=YOUR_TOKEN&base_url=lb2chb77ok&datalake_key=YOUR_LAKE_KEY&item=OrderLine
```

## Deployment

1. **Create Function App**:
   ```cmd
   az functionapp create --resource-group {resource_group} --consumption-plan-location {location} --runtime python --runtime-version 3.11 --functions-version 4 --name bigcommerce-downloader --storage-account {storage_account}
   ```

2. **Deploy Function**:
   ```cmd
   func azure functionapp publish bigcommerce-downloader --python --build remote
   ```

## BigCommerce API Limits

- **Rate Limits**: 20,000 API calls per hour per store
- **Page Size**: Maximum 250 items per request
- **Timeout**: 30 seconds per API call
- **Safety Limits**: Maximum 50 pages per request to prevent infinite loops

## Special Endpoint Handling

### Direct Catalog Endpoints
- **products, brands, categories, trees**: Fetched directly with pagination

### Product-Specific Endpoints
- **variants, options, images**: Requires product IDs, so function:
  1. First fetches all products
  2. Then fetches variants/options/images for each product
  3. Adds `product_id` field to each item for reference

## Notes

- Function includes automatic pagination handling
- Gets all current catalog data (no date filtering needed for products)
- All BigCommerce catalog endpoints supported
- Product-specific endpoints automatically include product_id reference
- Same Azure Data Lake configuration as Magento functions
- Comprehensive error handling and logging
