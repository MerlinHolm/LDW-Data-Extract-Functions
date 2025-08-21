@echo off
echo Creating Azure Function App...
az functionapp create ^
  --resource-group "prodbi" ^
  --consumption-plan-location "westUS2" ^
  --runtime python ^
  --runtime-version 3.11 ^
  --functions-version 4 ^
  --name "shopify-downloader" ^
  --storage-account "prodbimanager" ^
  --os-type Linux

echo Deploying function code...
func azure functionapp publish shopify-downloader --python --build remote

echo Deployment complete!
pause
