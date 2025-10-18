@echo off
echo Deploying Slack Downloader Azure Function...
echo.

REM Navigate to the slack_downloader directory
cd /d "c:\EcommDownloader\slack_downloader"

REM Deploy to Azure
func azure functionapp publish slack-downloader --python --force --build remote

echo.
echo Deployment complete!
echo Function URLs:
echo - get_channel_data: https://slack-downloader.azurewebsites.net/api/get_channel_data
echo - get_workspace_data: https://slack-downloader.azurewebsites.net/api/get_workspace_data
echo.
pause
