# Salesforce Commerce Cloud Downloader Function App

This Azure Function App downloads product and order data from Salesforce Commerce Cloud and saves it to Azure Data Lake Storage.

## Functions

This app provides two main functions:

1.  `get_product_data`: Downloads product catalog data.
2.  `get_order_data`: Downloads order, line, and shipment data.

---

## Function: `get_product_data`

Downloads product data from the Salesforce Commerce Cloud Product Search API. This includes product details, variations, images, and inventory. Pricing data is currently disabled but can be re-enabled.

### Required Parameters

-   `client_id`: Salesforce Commerce Cloud client ID.
-   `client_secret`: Salesforce Commerce Cloud client secret.
-   `datalake_key`: Azure Data Lake Storage access key.
-   `data_lake_path`: The full path in the Data Lake where the file will be saved (e.g., `Retail/products/input/files/json`).
-   `filename`: The name for the output file (e.g., `salesforce-products`).

### Optional Parameters

-   `base_url`: Salesforce Commerce Cloud domain (default: `kv7kzm78.api.commercecloud.salesforce.com`). The `https://` prefix is added automatically.
-   `api_version`: API version (default: `v1`).
-   `organization_id`: Organization ID (default: `f_ecom_zysr_001`).
-   `site_id`: Site ID (default: `RefArchGlobal`).
-   `page_size`: Number of records per API call (default: `200`).
-   `catalog_id`: **(New)** The ID of a specific catalog to filter products. If not provided, products from all catalogs are returned.
-   `price_book_id`: **(New)** The ID of a specific price book to get pricing from. *Note: Pricing logic is currently disabled.*

### Example Usage

```
GET /api/get_product_data?client_id=...&client_secret=...&datalake_key=...&data_lake_path=...&filename=...&catalog_id=my-catalog
```

---

## Function: `get_order_data`

Downloads order data from Salesforce Commerce Cloud using OAuth2 authentication.

### Required Parameters

-   `client_id`: Salesforce Commerce Cloud client ID.
-   `client_secret`: Salesforce Commerce Cloud client secret.
-   `datalake_key`: Azure Data Lake Storage access key.

### Optional Parameters

-   `base_url`: Salesforce Commerce Cloud domain (default: `kv7kzm78.api.commercecloud.salesforce.com`). The `https://` prefix is added automatically.
-   `api_version`: API version (default: `v1`).
-   `organization_id`: Organization ID (default: `f_ecom_zysr_001`).
-   `site_id`: Site ID (default: `RefArchUS`).
-   `limit`: Number of records per API call (default: `200`).
-   `data_lake_path`: Data Lake storage path (default: `RetailOrders/input/files/json/orders`).
-   `filename`: Filename prefix (default: `orders`).
-   `start_date`: Filter orders from this date (format: YYYY-MM-DD).
-   `end_date`: Filter orders to this date (format: YYYY-MM-DD).

### Authentication Flow

1.  Uses OAuth2 client credentials grant to obtain an access token.
2.  Calls the relevant Salesforce Commerce Cloud API with a bearer token.
3.  Handles pagination automatically to fetch all available data.

### API Endpoints Used

-   **Token Endpoint**: `https://account.demandware.com/dwsso/oauth2/access_token`
-   **Product Search API**: `/product/products/v1/organizations/{organization_id}/product-search`
-   **Orders API**: `/checkout/orders/v1/organizations/{organization_id}/orders`

### Example Usage

```
GET /api/get_order_data?client_id=...&client_secret=...&datalake_key=...&start_date=2024-01-01&end_date=2024-01-31
```

### Response Format

```json
{
  "status": "success",
  "message": "Successfully downloaded and saved data",
  "records_count": 150,
  "filename": "your-filename.json",
  "path": "your/data_lake_path"
}
```

### Error Handling

The function includes comprehensive error handling for:

-   Missing required parameters
-   OAuth2 authentication failures
-   API call failures
-   Data Lake storage errors
