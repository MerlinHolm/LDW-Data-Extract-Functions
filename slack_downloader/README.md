# Slack Downloader Azure Function

This Azure Function downloads Slack channel data and workspace information, saving it to Azure Data Lake as parquet files for analytics and reporting.

## Features

- **Channel Messages**: Download messages from specific channels or all accessible channels
- **Workspace Data**: Download team info, channels list, and users list
- **Parquet Format**: Data saved as efficient parquet files for analytics
- **Flexible Parameters**: Support for date ranges, pagination, and filtering
- **Multi-workspace Support**: Handle multiple Slack workspaces
- **Comprehensive Metadata**: Includes timestamps, user info, and channel context

## Deployment

### Prerequisites
- Azure CLI installed and configured
- Azure Functions Core Tools installed
- Python 3.9+ installed

### Deploy to Azure
```bash
cd c:\EcommDownloader\slack_downloader
func azure functionapp publish slack-downloader --python --force --build remote
```

Or run the provided batch file:
```bash
deploy.bat
```

## API Endpoints

### 1. Get Channel Data
**URL**: `https://slack-downloader.azurewebsites.net/api/get_channel_data`

Downloads messages and metadata from Slack channels.

#### Required Parameters
- `auth_token`: Slack Bot User OAuth Token (xoxb-...)
- `datalake_key`: Azure Data Lake connection string

#### Optional Parameters
- `channel`: Channel ID or name (if not provided, fetches all channels)
- `workspace`: Workspace/Team ID (for multi-workspace bots)
- `api_version`: Slack API version (default: v1)
- `data_lake_path`: Path in DataLake (default: 'Communication/Slack/Channels')
- `filename_prefix`: Prefix for saved files (default: 'slack')
- `oldest`: Oldest timestamp for messages (Unix timestamp)
- `latest`: Latest timestamp for messages (Unix timestamp)
- `limit`: Number of messages per request (default: 1000, max: 1000)
- `include_users`: Include user information (default: true)
- `include_channel_info`: Include channel metadata (default: true)

#### Example Usage
```bash
# Get all channel data
https://slack-downloader.azurewebsites.net/api/get_channel_data?auth_token=xoxb-your-token&datalake_key=your-key

# Get specific channel data
https://slack-downloader.azurewebsites.net/api/get_channel_data?auth_token=xoxb-your-token&datalake_key=your-key&channel=general

# Get data with date range
https://slack-downloader.azurewebsites.net/api/get_channel_data?auth_token=xoxb-your-token&datalake_key=your-key&oldest=1640995200&latest=1672531200
```

### 2. Get Workspace Data
**URL**: `https://slack-downloader.azurewebsites.net/api/get_workspace_data`

Downloads workspace-level information including team details, all channels, and all users.

#### Required Parameters
- `auth_token`: Slack Bot User OAuth Token (xoxb-...)
- `datalake_key`: Azure Data Lake connection string

#### Optional Parameters
- `workspace`: Workspace/Team ID (for multi-workspace bots)
- `api_version`: Slack API version (default: v1)
- `data_lake_path`: Path in DataLake (default: 'Communication/Slack/Workspace')
- `filename_prefix`: Prefix for saved files (default: 'slack_workspace')
- `include_archived`: Include archived channels (default: false)
- `include_private`: Include private channels (default: false)

#### Example Usage
```bash
# Get workspace data
https://slack-downloader.azurewebsites.net/api/get_workspace_data?auth_token=xoxb-your-token&datalake_key=your-key

# Include archived and private channels
https://slack-downloader.azurewebsites.net/api/get_workspace_data?auth_token=xoxb-your-token&datalake_key=your-key&include_archived=true&include_private=true
```

## Slack App Setup

### Required Scopes
Your Slack app needs the following OAuth scopes:

**Bot Token Scopes:**
- `channels:history` - Read messages from public channels
- `channels:read` - View basic information about public channels
- `groups:history` - Read messages from private channels (if needed)
- `groups:read` - View basic information about private channels (if needed)
- `users:read` - View people in the workspace
- `team:read` - View team information

### Installation Steps
1. Go to [Slack API](https://api.slack.com/apps)
2. Create a new app or select existing app
3. Go to "OAuth & Permissions"
4. Add the required scopes listed above
5. Install the app to your workspace
6. Copy the "Bot User OAuth Token" (starts with `xoxb-`)

## Data Structure

### Messages Data
Each message record includes:
- `ts`: Message timestamp
- `timestamp_readable`: Human-readable timestamp
- `user`: User ID who sent the message
- `text`: Message text content
- `channel_id`: Channel ID
- `channel_name`: Channel name
- `type`: Message type
- `subtype`: Message subtype (if any)
- Additional Slack message fields

### Channels Data
Each channel record includes:
- `id`: Channel ID
- `name`: Channel name
- `is_channel`: Boolean indicating if it's a channel
- `is_private`: Boolean indicating if it's private
- `is_archived`: Boolean indicating if it's archived
- `created`: Creation timestamp
- `creator`: User ID who created the channel
- `topic`: Channel topic
- `purpose`: Channel purpose
- Additional Slack channel fields

### Users Data
Each user record includes:
- `id`: User ID
- `name`: Username
- `real_name`: Real name
- `display_name`: Display name
- `email`: Email address (if available)
- `is_bot`: Boolean indicating if it's a bot
- `is_admin`: Boolean indicating admin status
- `profile`: User profile information
- Additional Slack user fields

## File Organization

Files are saved to Azure Data Lake with the following structure:
```
Communication/Slack/Channels/
├── slack_messages_YYYYMMDD_HHMMSS.parquet
├── slack_channels_YYYYMMDD_HHMMSS.parquet
└── slack_users_YYYYMMDD_HHMMSS.parquet

Communication/Slack/Workspace/
├── slack_workspace_team_info_YYYYMMDD_HHMMSS.parquet
├── slack_workspace_channels_YYYYMMDD_HHMMSS.parquet
└── slack_workspace_users_YYYYMMDD_HHMMSS.parquet
```

## Response Format

### Success Response
```json
{
  "status": "success",
  "message": "Processed Slack data for 3 data types",
  "channel": "general",
  "workspace": "DEFAULT",
  "files_saved": 3,
  "files_failed": 0,
  "save_results": [
    {
      "type": "messages",
      "result": {
        "success": true,
        "path": "Communication/Slack/Channels/slack_messages_20241008_141623.parquet",
        "filesystem": "files",
        "size_bytes": 15420,
        "records_count": 150
      }
    }
  ],
  "path": "Communication/Slack/Channels",
  "timestamp": "20241008_141623",
  "metadata": {
    "fetch_timestamp": "2024-10-08T21:16:23.456789+00:00",
    "channels_processed": 1,
    "total_messages": 150,
    "workspace": null
  }
}
```

### Error Response
```json
{
  "error": "MISSING_PARAMETER",
  "message": "auth_token parameter is required (Slack Bot User OAuth Token)"
}
```

## Rate Limits

Slack API has rate limits:
- Tier 1 methods: 1+ requests per minute
- Tier 2 methods: 20+ requests per minute
- Tier 3 methods: 50+ requests per minute
- Tier 4 methods: 100+ requests per minute

The function handles pagination automatically and respects rate limits.

## Security Notes

- Never expose your Slack Bot Token in URLs or logs
- Use Azure Key Vault for storing sensitive tokens
- Ensure your Data Lake has proper access controls
- Monitor function logs for any security issues

## Troubleshooting

### Common Issues

1. **"Channel not found" error**
   - Verify the channel name/ID is correct
   - Ensure the bot has access to the channel
   - Check if the channel is private and bot has appropriate permissions

2. **"Invalid auth" error**
   - Verify the auth_token is correct and starts with `xoxb-`
   - Check if the token has expired
   - Ensure required scopes are granted

3. **"Missing scope" error**
   - Add the required OAuth scopes to your Slack app
   - Reinstall the app to your workspace

4. **Data Lake connection issues**
   - Verify the datalake_key connection string is correct
   - Check Azure Data Lake permissions
   - Ensure the 'files' container exists

### Logs
Check Azure Function logs in the Azure portal for detailed error information.

## Version History

- **v1.0**: Initial release with channel and workspace data download
- Support for parquet file format
- Multi-workspace support
- Comprehensive error handling
