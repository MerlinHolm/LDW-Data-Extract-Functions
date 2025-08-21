# Shopify Product Data Downloader

Azure Function App for downloading comprehensive product catalog data from Shopify using the Admin GraphQL API.

## Overview

This function downloads complete product data from Shopify stores including:
- Product details (title, vendor, type, status, etc.)
- Product variants with pricing and inventory
- Product options and selected options
- Product media and images
- Collections and categories
- Comprehensive metadata

## Function Details

- **Function App Name**: `shopify-downloader`
- **Function Name**: `get_product_data`
- **Runtime**: Python 3.11
- **Trigger**: HTTP
- **Authentication**: Function Key Required

## Parameters

### Required Parameters

- **`auth_token`**: Shopify Admin API access token
- **`base_url`**: Shopify store domain (e.g., `dearfoams-costco-next`)
- **`datalake_key`**: Azure Data Lake Storage access key

### Optional Parameters

- **`api_version`**: Shopify API version (default: `2024-10`)
- **`filename`**: Filename prefix for saved data (default: `shopify`)
- **`page_size`**: Number of products per GraphQL query (default: `250`)
- **`data_lake_path`**: Azure Data Lake path (default: `RetailProducts/input/files/json/products/base`)

## GraphQL Query

The function uses a comprehensive GraphQL query to retrieve:

```graphql
query {
  products(first: 250) {
    edges {
      node {
        id
        title
        category {
          name
          fullName
        }
        collections(first: 10) {
          edges {
            node {
              title
            }
          }
        }
        vendor
        productType
        totalInventory
        createdAt
        handle
        updatedAt
        publishedAt
        tags
        status
        variants(first: 100) {
          edges {
            node {
              id
              title
              sku
              displayName
              price
              position
              compareAtPrice
              selectedOptions {
                name
                value
              }
              createdAt
              updatedAt
              taxable
              barcode
              inventoryQuantity
              product {
                id
              }
              image {
                id
                altText
                url
                width
                height
              }
            }
          }
        }
        options {
          id
          name
          position
          values
        }
        media(first: 100) {
          edges {
            node {
              id
              preview {
                image {
                  url
                }
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasPreviousPage
      hasNextPage
      startCursor
      endCursor
    }
  }
}
```

## URL Construction

The function constructs the Shopify GraphQL URL as follows:
- **Pattern**: `https://{base_url}.myshopify.com/admin/api/{api_version}/graphql.json`
- **Example**: `https://dearfoams-costco-next.myshopify.com/admin/api/2024-10/graphql.json`

## Azure Data Lake Integration

- **Account**: prodbimanager
- **File System**: prodbidlstorage
- **Default Path**: RetailProducts/input/files/json/products/base
- **Filename Format**: `{filename_prefix}-products.json`

## Response Format

### Success Response (with data)
```json
{
  "status": "success",
  "message": "Successfully downloaded and saved products data",
  "records_count": 150,
  "filename": "shopify.CXR045-products.json",
  "path": "RetailProducts/input/files/json/products/base"
}
```

### Success Response (no data found)
```json
{
  "status": "success",
  "message": "No products found for this store - empty file created",
  "records_count": 0,
  "filename": "shopify.CXR045-products.json",
  "path": "RetailProducts/input/files/json/products/base",
  "note": "Empty file created to indicate endpoint was successfully checked"
}
```

### Error Response
```json
{
  "error": "GRAPHQL_API_ERROR",
  "message": "Shopify GraphQL API returned status 401",
  "details": "Unauthorized access"
}
```

## Authentication

The function uses Shopify Admin API access tokens passed via the `X-Shopify-Access-Token` header.

### Required Shopify API Permissions

Your Shopify app needs the following scopes:
- `read_products`
- `read_product_listings`
- `read_inventory`

## Pagination

The function automatically handles GraphQL cursor-based pagination:
- Fetches up to 50 pages (safety limit)
- Uses `pageInfo` cursors for navigation
- Combines all pages into a single result set

## Example Usage

### Basic Usage
```
https://shopify-downloader.azurewebsites.net/api/get_product_data?code=YOUR_FUNCTION_KEY&auth_token=YOUR_SHOPIFY_TOKEN&base_url=dearfoams-costco-next&datalake_key=YOUR_DATALAKE_KEY&filename=shopify.CXR045
```

### With Custom Parameters
```
https://shopify-downloader.azurewebsites.net/api/get_product_data?code=YOUR_FUNCTION_KEY&auth_token=YOUR_SHOPIFY_TOKEN&base_url=dearfoams-costco-next&api_version=2024-10&page_size=100&datalake_key=YOUR_DATALAKE_KEY&data_lake_path=RetailProducts/input/files/json/products/base&filename=shopify.CXR045
```

## Data Structure

The saved JSON file contains:

```json
{
  "data": [
    {
      "id": "gid://shopify/Product/123456789",
      "title": "Sample Product",
      "vendor": "Sample Vendor",
      "productType": "Sample Type",
      "handle": "sample-product",
      "status": "ACTIVE",
      "totalInventory": 100,
      "createdAt": "2024-01-01T00:00:00Z",
      "updatedAt": "2024-01-01T00:00:00Z",
      "publishedAt": "2024-01-01T00:00:00Z",
      "tags": ["tag1", "tag2"],
      "variants": {
        "edges": [
          {
            "node": {
              "id": "gid://shopify/ProductVariant/987654321",
              "title": "Default Title",
              "sku": "SAMPLE-SKU-001",
              "price": "29.99",
              "compareAtPrice": "39.99",
              "inventoryQuantity": 50,
              "barcode": "1234567890123",
              "selectedOptions": [
                {
                  "name": "Size",
                  "value": "Medium"
                }
              ]
            }
          }
        ]
      },
      "options": [
        {
          "id": "gid://shopify/ProductOption/111111111",
          "name": "Size",
          "position": 1,
          "values": ["Small", "Medium", "Large"]
        }
      ],
      "collections": {
        "edges": [
          {
            "node": {
              "title": "Featured Products"
            }
          }
        ]
      },
      "media": {
        "edges": [
          {
            "node": {
              "id": "gid://shopify/MediaImage/222222222",
              "preview": {
                "image": {
                  "url": "https://cdn.shopify.com/s/files/1/0000/0000/0000/products/image.jpg"
                }
              }
            }
          }
        ]
      }
    }
  ],
  "total_count": 1,
  "metadata": {
    "source": "https://dearfoams-costco-next.myshopify.com/admin/api/2024-10/graphql.json",
    "query_type": "GraphQL",
    "pages_fetched": 1,
    "page_size": "250",
    "has_more_pages": false
  }
}
```

## Error Handling

The function includes comprehensive error handling for:
- Missing required parameters
- Invalid Shopify access tokens
- GraphQL query errors
- Network connectivity issues
- Azure Data Lake storage failures
- JSON parsing errors

## Deployment

### Prerequisites
- Azure CLI installed and configured
- Azure Functions Core Tools installed
- Python 3.11 runtime

### Deploy to Azure
```bash
# Run the deployment script
deploy.bat
```

### Manual Deployment
```bash
# Set subscription
az account set --subscription "Pay-As-You-Go"

# Create Function App
az functionapp create \
  --resource-group prodbi \
  --consumption-plan-location "East US" \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name shopify-downloader \
  --storage-account prodbimanager \
  --os-type Linux

# Deploy function code
func azure functionapp publish shopify-downloader --python --build remote
```

## Testing

### Test URL Template
```
https://shopify-downloader.azurewebsites.net/api/get_product_data?code={FUNCTION_KEY}&auth_token={SHOPIFY_TOKEN}&base_url={STORE_DOMAIN}&datalake_key={DATALAKE_KEY}&filename={FILENAME_PREFIX}
```

### Example Test
```
https://shopify-downloader.azurewebsites.net/api/get_product_data?code=ABC123&auth_token=shpat_xyz&base_url=dearfoams-costco-next&datalake_key=YOUR_KEY&filename=shopify.CXR045
```

## Monitoring

- Function execution logs are available in Azure Application Insights
- Data Lake file creation can be monitored through Azure Storage Explorer
- GraphQL query performance is logged for optimization

## Troubleshooting

### Common Issues

1. **401 Unauthorized**: Check Shopify access token and permissions
2. **GraphQL Errors**: Verify API version compatibility and query syntax
3. **Data Lake Errors**: Confirm storage account access and permissions
4. **Timeout Errors**: Consider reducing page_size for large catalogs

### Debug Information

The function provides detailed logging for:
- GraphQL request/response details
- Pagination progress
- Data Lake save operations
- Error stack traces

## Version History

- **v1.0**: Initial release with comprehensive Shopify GraphQL product data download
- Supports pagination, error handling, and Azure Data Lake integration
- Compatible with Shopify Admin API 2024-10 and later versions
