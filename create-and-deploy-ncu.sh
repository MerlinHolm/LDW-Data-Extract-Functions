#!/bin/bash

# Complete script to create and deploy all NCU function apps
# This combines both creation and deployment in one script

# Configuration - UPDATED FOR PRODBI2
RESOURCE_GROUP="prodbi2"
STORAGE_ACCOUNT="prodbidlstorage"          # Update if different
LOCATION="northcentralus"
PLAN_NAME="flex-consumption-plan-ncu"

# Function app configurations
declare -A FUNCTION_APPS=(
    ["bigcommerce-downloader-ncu"]="bigcommerce_downloader"
    ["shopify-downloader-ncu"]="shopify_downloader"
    ["salesforce-downloader-ncu"]="salesforce_downloader"
    ["magento-downloader-ncu"]="magento_downloader"
    ["monday-downloader-ncu"]="monday_downloader"
    ["slack-downloader-ncu"]="slack_downloader"
)

echo "🚀 Creating and deploying function apps in North Central US..."
echo "Location: $LOCATION"
echo "Resource Group: $RESOURCE_GROUP"
echo ""

# Step 1: Create Flex Consumption Plan (avoids Linux Consumption EOL warning)
echo "📋 Step 1: Creating Flex Consumption Plan: $PLAN_NAME"
az functionapp plan create \
    --resource-group $RESOURCE_GROUP \
    --name $PLAN_NAME \
    --location $LOCATION \
    --sku FC1 \
    --min-instances 0 \
    --max-instances 1000

echo ""

# Step 2: Create and deploy each function app
for app_name in "${!FUNCTION_APPS[@]}"; do
    folder_name="${FUNCTION_APPS[$app_name]}"
    
    echo "🔧 Creating function app: $app_name"
    
    # Create the function app using Flex Consumption
    az functionapp create \
        --resource-group $RESOURCE_GROUP \
        --plan $PLAN_NAME \
        --runtime python \
        --runtime-version 3.11 \
        --functions-version 4 \
        --name $app_name \
        --storage-account $STORAGE_ACCOUNT \
        --os-type Linux
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created: $app_name"
        
        # Wait a moment for the app to be ready
        echo "⏳ Waiting for app to be ready..."
        sleep 30
        
        # Deploy the function
        echo "🚀 Deploying $folder_name to $app_name..."
        
        if [ -d "$folder_name" ]; then
            cd "$folder_name"
            
            func azure functionapp publish "$app_name" --python --force --build remote
            
            if [ $? -eq 0 ]; then
                echo "✅ Successfully deployed: $app_name"
            else
                echo "❌ Failed to deploy: $app_name"
            fi
            
            cd ..
        else
            echo "❌ Directory not found: $folder_name"
        fi
    else
        echo "❌ Failed to create: $app_name"
    fi
    
    echo "---"
done

echo ""
echo "🎉 All function apps created and deployed!"
echo ""
echo "📍 Function app URLs (North Central US):"
echo "• BigCommerce: https://bigcommerce-downloader-ncu.azurewebsites.net/api/get_product_data"
echo "• Shopify: https://shopify-downloader-ncu.azurewebsites.net/api/get_order_data"
echo "• Salesforce: https://salesforce-downloader-ncu.azurewebsites.net/api/get_order_data"
echo "• Magento: https://magento-downloader-ncu.azurewebsites.net/api/get_magento_data"
echo "• Monday: https://monday-downloader-ncu.azurewebsites.net/api/get_board_data"
echo "• Slack: https://slack-downloader-ncu.azurewebsites.net/api/get_channel_data"
echo ""
echo "⚠️  Remember to update the configuration variables at the top of this script before running!"
