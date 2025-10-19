#!/bin/bash

# Azure CLI script to deploy all function apps to North Central US
# Run this after creating the function apps with create-ncu-function-apps.sh

# Function app deployments with -ncu suffix
DEPLOYMENTS=(
    "bigcommerce_downloader:bigcommerce-downloader-ncu"
    "shopify_downloader:shopify-downloader-ncu"
    "salesforce_downloader:salesforce-downloader-ncu"
    "magento_downloader:magento-downloader-ncu"
    "monday_downloader:monday-downloader-ncu"
    "slack_downloader:slack-downloader-ncu"
)

echo "Deploying function apps to North Central US..."
echo "Current directory: $(pwd)"
echo ""

# Deploy each function app
for deployment in "${DEPLOYMENTS[@]}"; do
    # Split the deployment string
    IFS=':' read -r folder app_name <<< "$deployment"
    
    echo "🚀 Deploying $folder to $app_name..."
    echo "Changing to directory: $folder"
    
    if [ -d "$folder" ]; then
        cd "$folder"
        
        echo "Running: func azure functionapp publish $app_name --python --force --build remote"
        
        func azure functionapp publish "$app_name" --python --force --build remote
        
        if [ $? -eq 0 ]; then
            echo "✅ Successfully deployed: $app_name"
        else
            echo "❌ Failed to deploy: $app_name"
        fi
        
        cd ..
    else
        echo "❌ Directory not found: $folder"
    fi
    
    echo "---"
done

echo ""
echo "Deployment complete!"
echo ""
echo "Function app URLs:"
echo "• BigCommerce: https://bigcommerce-downloader-ncu.azurewebsites.net/api/get_product_data"
echo "• Shopify: https://shopify-downloader-ncu.azurewebsites.net/api/get_order_data"
echo "• Salesforce: https://salesforce-downloader-ncu.azurewebsites.net/api/get_order_data"
echo "• Magento: https://magento-downloader-ncu.azurewebsites.net/api/get_magento_data"
echo "• Monday: https://monday-downloader-ncu.azurewebsites.net/api/get_board_data"
echo "• Slack: https://slack-downloader-ncu.azurewebsites.net/api/get_channel_data"
