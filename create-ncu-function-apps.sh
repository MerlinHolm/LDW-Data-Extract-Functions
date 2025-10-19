#!/bin/bash

# Azure CLI script to create function apps in North Central US with -ncu suffix
# Run this script to create all the function apps before deploying

# Configuration
RESOURCE_GROUP="prodbi2"
STORAGE_ACCOUNT="prodbidlstorage"          # Update if different
LOCATION="northcentralus"
PLAN_NAME="flex-consumption-plan-ncu"

# Function app names with -ncu suffix
FUNCTION_APPS=(
    "bigcommerce-downloader-ncu"
    "shopify-downloader-ncu" 
    "salesforce-downloader-ncu"
    "magento-downloader-ncu"
    "monday-downloader-ncu"
    "slack-downloader-ncu"
)

echo "Creating function apps in North Central US..."

# Create Flex Consumption Plan (recommended to avoid EOL warning)
echo "Creating Flex Consumption Plan: $PLAN_NAME"
az functionapp plan create \
    --resource-group $RESOURCE_GROUP \
    --name $PLAN_NAME \
    --location $LOCATION \
    --sku FC1 \
    --min-instances 0 \
    --max-instances 1000

# Create each function app using Flex Consumption
for app in "${FUNCTION_APPS[@]}"; do
    echo "Creating function app: $app"
    
    az functionapp create \
        --resource-group $RESOURCE_GROUP \
        --plan $PLAN_NAME \
        --runtime python \
        --runtime-version 3.11 \
        --functions-version 4 \
        --name $app \
        --storage-account $STORAGE_ACCOUNT \
        --os-type Linux
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created: $app"
    else
        echo "❌ Failed to create: $app"
    fi
    
    echo "---"
done

echo "Function app creation complete!"
echo ""
echo "Next steps:"
echo "1. Update the resource group, storage account, and plan names in this script"
echo "2. Run: chmod +x create-ncu-function-apps.sh"
echo "3. Run: ./create-ncu-function-apps.sh"
echo "4. Then run the deployment script: ./deploy-ncu-functions.sh"
