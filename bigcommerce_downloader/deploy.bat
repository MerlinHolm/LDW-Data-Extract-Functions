@echo off
echo Setting Azure subscription...
az account set --subscription "Pay-As-You-Go"

echo Creating BigCommerce downloader Function App...
az functionapp create ^
    --resource-group "rg-prod-bi-manager" ^
    --consumption-plan-location "West US 2" ^
    --runtime python ^
    --runtime-version 3.11 ^
    --functions-version 4 ^
    --name "bigcommerce-downloader" ^
    --storage-account "prodbimanager"

echo Deploying function code...
func azure functionapp publish bigcommerce-downloader --python --build remote

echo Deployment complete!
pause
