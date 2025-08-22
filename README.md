# EcommDownloader - Master Project

This is the **master project** containing all ecommerce platform data downloaders.

## Overview

The EcommDownloader project contains Azure Function apps for downloading data from multiple ecommerce platforms. All functions follow standardized patterns for authentication, data extraction, and storage while adapting to each platform's specific API requirements.

## Platform Support

| Platform | Functions | Data Types | Authentication | API Type |
|----------|-----------|------------|----------------|----------|
| **Monday.com** | `get_board_data`, `get_file_data` | Board metadata, CXR CSV files | API Token | GraphQL |
| **Magento** | `get_magento_data`, `get_magento_products` | Orders, Shipments, Products, Categories | Bearer Token | REST |
| **BigCommerce** | `get_product_data` | Products, Brands, Categories, Variants | X-Auth-Token | REST v3 |
| **Salesforce** | `get_product_data`, `get_order_data` | Products, Orders | OAuth2 Client Credentials | REST |
| **Shopify** | `get_product_data` | Products with Variants/Collections | X-Shopify-Access-Token | GraphQL |

## Common Architecture

### Data Lake Storage
- **Account**: `prodbimanager`
- **Filesystem**: `prodbidlstorage`
- **Paths**: Platform-specific (e.g., `RetailProducts/input/files/json/products/base`)

### Response Format
All functions return consistent JSON responses:
```json
{
  "status": "success|error",
  "message": "Description of operation",
  "records_count": 123,
  "filename": "data-file.json",
  "path": "storage/path"
}
```

### Error Handling
- Detailed error messages with debug information
- API response status codes and error details
- Graceful handling of empty results

## Platform Differences

### Authentication Methods
- **Monday.com**: Direct API token in Authorization header
- **Magento**: Bearer token in Authorization header
- **BigCommerce**: X-Auth-Token header
- **Salesforce**: OAuth2 flow (client_id/client_secret → Bearer token)
- **Shopify**: X-Shopify-Access-Token header

### Pagination Strategies
- **Monday.com**: Single query (limit 500 items)
- **Magento**: Page-based with searchCriteria
- **BigCommerce**: Page-based with meta.pagination
- **Salesforce**: Offset-based pagination
- **Shopify**: Cursor-based GraphQL pagination

### File Naming Conventions
- **Monday.com**: `{itemID}-{assetID}.csv` for files, `{boardID}.json` for metadata
- **Magento**: `{filename}.YYYYMMDD-{item}.json` (orders), `{filename}-{item}.json` (products)
- **BigCommerce**: `{filename}-{item}.json`
- **Salesforce**: `{filename}.json` (products), `{filename}.YYYYMMDD-orders.json` (orders)
- **Shopify**: `{filename}-products.json`

## Detailed Platform Patterns

### Monday.com Downloader Pattern
- **Functions**: `get_board_data` (board metadata), `get_file_data` (CXR CSV files)
- **Authentication**: API Token in Authorization header
- **API**: GraphQL with single query (limit 500 items)
- **File Filtering**: Asset name starts with "CXR" AND extension is ".csv"
- **Storage**: JSON metadata `MondayBoards/input/files/json/boards/{boardID}.json`, CSV files `MondayBoards/input/files/csv/{boardID}/{itemID}-{assetID}.csv`
- **Parameters**: `boardID`, `api_token`, `datalake_key`

### Magento Downloader Pattern
- **Functions**: `get_magento_data` (orders/shipments), `get_magento_products` (products/categories/stockitems)
- **Authentication**: Bearer token in Authorization header
- **API**: REST API with searchCriteria filters
- **Date Filtering**: Two modes - 'from_to' and 'gteq_lteq' for different Magento instances
- **Pagination**: Page-based with configurable page_size (default 50)
- **Storage**: Daily files for orders `{filename}.YYYYMMDD-{item}.json`, single files for products `{filename}-{item}.json`
- **Parameters**: `auth_token`, `datalake_key`, `base_url`, `channel`, `store_id`, `start_date`, `end_date`, `filter_type`

### BigCommerce Downloader Pattern
- **Function**: `get_product_data` (products/brands/categories/variants/options/images)
- **Authentication**: X-Auth-Token header
- **API**: REST API v3 with pagination
- **Endpoints**: Direct catalog endpoints + product-specific endpoints requiring product IDs
- **Pagination**: Page-based with meta.pagination info
- **Storage**: Single files `{filename}-{item}.json`
- **Parameters**: `auth_token`, `datalake_key`, `base_url` (store hash), `item`, `page_size`

### Salesforce Downloader Pattern
- **Functions**: `get_product_data` (products), `get_order_data` (orders)
- **Authentication**: OAuth2 client credentials flow → Bearer token
- **API**: Commerce Cloud REST API (Product Search and Orders)
- **Pagination**: Offset-based with configurable limit
- **Storage**: Single file for products (`{filename}.json`), date-based for orders (`{filename}.YYYYMMDD-orders.json`)
- **Parameters**:
    - **Common**: `client_id`, `client_secret`, `datalake_key`, `base_url`, `organization_id`, `site_id`
    - **Products**: `data_lake_path`, `filename`, `page_size`, `catalog_id`, `price_book_id`
    - **Orders**: `limit`, `data_lake_path`, `filename`, `start_date`, `end_date`

### Shopify Downloader Pattern
- **Function**: `get_product_data` (products only)
- **Authentication**: X-Shopify-Access-Token header
- **API**: GraphQL API with cursor-based pagination
- **Query**: Complex nested GraphQL with variants, collections, media
- **Pagination**: Cursor-based using pageInfo.endCursor
- **Storage**: Single files `{filename}-products.json`
- **Parameters**: `auth_token`, `datalake_key`, `base_url` (shop name), `api_version`, `page_size`

### Common Implementation Patterns
- **Azure Data Lake Storage**: Account `prodbimanager`, filesystem `prodbidlstorage`
- **JSON Format**: Metadata including source, timestamps, counts
- **Error Handling**: Detailed debug information with API response details
- **Empty File Creation**: When no data found (except Salesforce)
- **Response Format**: Consistent `status`, `message`, `records_count`, `filename`, `path`
- **Save Function**: All use same `save_to_datalake` function pattern

## Project Structure

```
EcommDownloader/
├── .venv/                     # Shared Python virtual environment
├── bigcommerce_downloader/    # BigCommerce product data downloader
├── magento_downloader/        # Magento orders and product data downloader
├── salesforce_downloader/     # Salesforce Commerce Cloud order data downloader
├── shopify_downloader/        # Shopify product data downloader
├── monday_downloader/         # Monday.com board and file data downloader
├── package.json              # NPM scripts for managing all downloaders
├── project.json              # Master project configuration
└── README.md                 # This file
```

## Getting Started

1. **Activate the shared virtual environment:**
   ```bash
   .venv\Scripts\activate
   ```

2. **Navigate to specific downloader:**
   ```bash
   cd monday_downloader
   # or cd magento_downloader, etc.
   ```

3. **Deploy or test the function app**

## NPM Scripts

Manage all downloaders from the root level:

- `npm run deploy:all` - Deploy all downloader functions
- `npm run test:all` - Test all downloader functions
- `npm run build:all` - Build all downloader functions

## Individual Downloader Documentation

Each downloader has comprehensive documentation:

- [Monday.com Downloader](./monday_downloader/README.md) - Board data and CXR CSV file extraction
- [Magento Downloader](./magento_downloader/README.md) - Orders, shipments, and product data
- [BigCommerce Downloader](./bigcommerce_downloader/README.md) - Product catalog data
- [Salesforce Downloader](./salesforce_downloader/README.md) - Product and order data
- [Shopify Downloader](./shopify_downloader/README.md) - Product data with GraphQL

## Upcoming Extensions

**Planned Functions:**
- Shopify order functions
- BigCommerce order functions  

All new functions will follow the established patterns for their respective platforms.
