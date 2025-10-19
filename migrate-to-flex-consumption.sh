#!/bin/bash

# Script to migrate existing function apps from Linux Consumption to Flex Consumption
# This addresses the EOL warning: "Linux Consumption will reach EOL on September 30 2028"

# Configuration
RESOURCE_GROUP="prodbi2"
LOCATION="northcentralus"  # Or your current region
NEW_PLAN_NAME="flex-consumption-plan-main"

# Existing function apps to migrate
EXISTING_APPS=(
    "bigcommerce-downloader"
    "shopify-downloader" 
    "salesforce-downloader"
    "magento-downloader"
    "monday-downloader"
    "slack-downloader"
)

echo "🔄 Migrating existing function apps to Flex Consumption..."
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo ""

# Step 1: Create new Flex Consumption plan
echo "📋 Creating new Flex Consumption plan: $NEW_PLAN_NAME"
az functionapp plan create \
    --resource-group $RESOURCE_GROUP \
    --name $NEW_PLAN_NAME \
    --location $LOCATION \
    --sku FC1 \
    --min-instances 0 \
    --max-instances 1000

if [ $? -eq 0 ]; then
    echo "✅ Successfully created Flex Consumption plan"
else
    echo "❌ Failed to create Flex Consumption plan"
    exit 1
fi

echo ""

# Step 2: Migrate each function app
for app in "${EXISTING_APPS[@]}"; do
    echo "🔄 Migrating function app: $app"
    
    # Move app to new plan
    az functionapp update \
        --resource-group $RESOURCE_GROUP \
        --name $app \
        --plan $NEW_PLAN_NAME
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully migrated: $app"
    else
        echo "❌ Failed to migrate: $app"
    fi
    
    echo "---"
done

echo ""
echo "🎉 Migration complete!"
echo ""
echo "📍 Migrated function apps (now on Flex Consumption):"
echo "• BigCommerce: https://bigcommerce-downloader.azurewebsites.net"
echo "• Shopify: https://shopify-downloader.azurewebsites.net"
echo "• Salesforce: https://salesforce-downloader.azurewebsites.net"
echo "• Magento: https://magento-downloader.azurewebsites.net"
echo "• Monday: https://monday-downloader.azurewebsites.net"
echo "• Slack: https://slack-downloader.azurewebsites.net"
echo ""
echo "⚠️  Note: You can now delete old consumption plans if they're empty"
echo "💰 Flex Consumption provides better cost optimization and avoids EOL warnings"
