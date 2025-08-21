# Salesforce Commerce Cloud Downloader Function App

This Azure Function App downloads orders, lines, and shipments data from Salesforce Commerce Cloud and saves it to Azure Data Lake Storage.

## Function: get-order-data

Downloads order data from Salesforce Commerce Cloud using OAuth2 authentication.

### Required Parameters

- `client_id`: Salesforce Commerce Cloud client ID
- `client_secret`: Salesforce Commerce Cloud client secret  
- `datalake_key`: Azure Data Lake Storage access key

### Optional Parameters

- `base_url`: Salesforce Commerce Cloud domain (default: `kv7kzm78.api.commercecloud.salesforce.com`) - https:// prefix is added automatically
- `api_version`: API version (default: `v1`)
- `organization_id`: Organization ID (default: `f_ecom_zysr_001`)
- `site_id`: Site ID (default: `RefArchUS`)
- `limit`: Number of records per API call (default: `200`)
- `data_lake_path`: Data Lake storage path (default: `RetailOrders/input/files/json/orders`)
- `filename`: Filename prefix (default: `orders`)
- `start_date`: Filter orders from this date (format: YYYY-MM-DD)
- `end_date`: Filter orders to this date (format: YYYY-MM-DD)

### Authentication Flow

1. Uses OAuth2 client credentials grant to obtain access token
2. Calls Salesforce Commerce Cloud API with bearer token
3. Handles pagination automatically to fetch all available data

### API Endpoints Used

- **Token Endpoint**: `https://account.demandware.com/dwsso/oauth2/access_token`
- **Orders Endpoint**: `https://kv7kzm78.api.commercecloud.salesforce.com/checkout/orders/v1/organizations/{organization_id}/orders`

### OAuth2 Scopes

- `SALESFORCE_COMMERCE_API:zysr_001`
- `sfcc.orders.rw`
- `sfcc.products`

### Example Usage

```
GET /api/get-order-data?client_id=YOUR_CLIENT_ID&client_secret=YOUR_CLIENT_SECRET&datalake_key=YOUR_DATALAKE_KEY&start_date=2024-01-01&end_date=2024-01-31
```

### Response Format

```json
{
  "status": "success",
  "message": "Successfully downloaded and saved order data",
  "records_count": 150,
  "filename": "orders_20240113_174500.json",
  "path": "RetailOrders/input/files/json/orders"
}
```

### Data Structure

The function fetches orders with all associated lines and shipments in a single API call. The data includes:

- Order details (order ID, customer info, totals, etc.)
- Line items (products, quantities, prices, etc.)
- Shipment information (tracking, delivery status, etc.)

### Deployment

1. Deploy as an Azure Function App
2. Configure application settings with your Salesforce credentials
3. Set up Azure Data Lake Storage connection
4. Test the function with required parameters

### Error Handling

The function includes comprehensive error handling for:
- Missing required parameters
- OAuth2 authentication failures
- API call failures
- Data Lake storage errors
- Network timeouts and retries
