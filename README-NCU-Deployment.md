# North Central US Function Apps Deployment

This guide helps you create and deploy function apps in North Central US with `-ncu` suffix.

## Prerequisites

1. **Azure CLI installed and logged in**
   ```bash
   az login
   ```

2. **Azure Functions Core Tools installed**
   ```bash
   npm install -g azure-functions-core-tools@4 --unsafe-perm true
   ```

3. **Update configuration** in the scripts with your Azure resource details

## Function Apps to be Created

| Original App | NCU App | Folder |
|-------------|---------|---------|
| `bigcommerce-downloader` | `bigcommerce-downloader-ncu` | `bigcommerce_downloader` |
| `shopify-downloader` | `shopify-downloader-ncu` | `shopify_downloader` |
| `salesforce-downloader` | `salesforce-downloader-ncu` | `salesforce_downloader` |
| `magento-downloader` | `magento-downloader-ncu` | `magento_downloader` |
| `monday-downloader` | `monday-downloader-ncu` | `monday_downloader` |
| `slack-downloader` | `slack-downloader-ncu` | `slack_downloader` |

## Deployment Options

### Option 1: Complete Deployment (Recommended)

1. **Update configuration** in `create-and-deploy-ncu.sh`:
   ```bash
   RESOURCE_GROUP="your-resource-group-name"
   STORAGE_ACCOUNT="your-storage-account"
   PLAN_NAME="your-app-service-plan-ncu"
   ```

2. **Make executable and run**:
   ```bash
   chmod +x create-and-deploy-ncu.sh
   ./create-and-deploy-ncu.sh
   ```

### Option 2: Step-by-Step Deployment

1. **Create function apps first**:
   ```bash
   chmod +x create-ncu-function-apps.sh
   # Update configuration in the script first
   ./create-ncu-function-apps.sh
   ```

2. **Deploy functions**:
   ```bash
   chmod +x deploy-ncu-functions.sh
   ./deploy-ncu-functions.sh
   ```

## Configuration Required

Before running any script, update these values:

- `RESOURCE_GROUP`: Your Azure resource group name
- `STORAGE_ACCOUNT`: Your Azure storage account name  
- `PLAN_NAME`: Name for the new App Service Plan in NCU

## Expected Results

After successful deployment, you'll have:

### Function App URLs (North Central US):
- **BigCommerce**: `https://bigcommerce-downloader-ncu.azurewebsites.net/api/get_product_data`
- **Shopify**: `https://shopify-downloader-ncu.azurewebsites.net/api/get_order_data`
- **Salesforce**: `https://salesforce-downloader-ncu.azurewebsites.net/api/get_order_data`
- **Magento**: `https://magento-downloader-ncu.azurewebsites.net/api/get_magento_data`
- **Monday**: `https://monday-downloader-ncu.azurewebsites.net/api/get_board_data`
- **Slack**: `https://slack-downloader-ncu.azurewebsites.net/api/get_channel_data`

## Troubleshooting

### Common Issues:

1. **Permission errors**: Ensure you're logged into Azure CLI with proper permissions
2. **Resource group not found**: Verify the resource group name exists
3. **Storage account issues**: Ensure the storage account exists and is accessible
4. **Function deployment fails**: Check that the folder names match exactly

### Manual Deployment (if scripts fail):

```bash
# Navigate to each folder and deploy manually
cd bigcommerce_downloader
func azure functionapp publish bigcommerce-downloader-ncu --python --force --build remote

cd ../shopify_downloader  
func azure functionapp publish shopify-downloader-ncu --python --force --build remote

# Repeat for each function app...
```

## Verification

Test each function app after deployment:

```bash
# Test BigCommerce
curl "https://bigcommerce-downloader-ncu.azurewebsites.net/api/get_product_data?code=YOUR_FUNCTION_KEY"

# Test Shopify
curl "https://shopify-downloader-ncu.azurewebsites.net/api/get_order_data?code=YOUR_FUNCTION_KEY"

# Continue testing other functions...
```

## Notes

- All function apps will be created in **North Central US** region
- Function apps use **Linux** hosting with **Python 3.11** runtime
- **Consumption plan** is used for cost efficiency
- Same function code is deployed to NCU apps (no code changes needed)
