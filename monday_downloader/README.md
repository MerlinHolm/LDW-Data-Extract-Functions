# Monday.com Downloader

Azure Function App for downloading board metadata and CXR CSV files from Monday.com boards.

## Functions

### 1. `get_board_data` - Board Metadata Downloader

Downloads complete board structure and metadata as JSON.

**URL**: `https://monday-downloader.azurewebsites.net/api/get_board_data`

**Parameters**:
```json
{
  "boardID": "4598613914",
  "api_token": "your_monday_api_token",
  "datalake_key": "your_azure_datalake_key"
}
```

**Response**:
```json
{
  "status": "success",
  "message": "Board data downloaded successfully",
  "json_file": "MondayBoards/input/files/json/boards/4598613914.json",
  "board_id": "4598613914"
}
```

### 2. `get_file_data` - CXR CSV File Downloader

Downloads CXR CSV files from board item assets.

**URL**: `https://monday-downloader.azurewebsites.net/api/get_file_data`

**Parameters**:
```json
{
  "boardID": "4598613914",
  "api_token": "your_monday_api_token", 
  "datalake_key": "your_azure_datalake_key"
}
```

**Response**:
```json
{
  "status": "success",
  "message": "CSV files downloaded successfully",
  "csv_files_downloaded": 113,
  "board_id": "4598613914"
}
```

## Process Flow

### get_board_data Flow
1. **Validate Parameters** → `boardID`, `api_token`, `datalake_key`
2. **GraphQL Query** → Fetch complete board structure
3. **Save to Data Lake** → `MondayBoards/input/files/json/boards/{boardID}.json`
4. **Return Response** → Success/error with file path

### get_file_data Flow  
1. **Validate Parameters** → `boardID`, `api_token`, `datalake_key`
2. **GraphQL Query** → Fetch items with assets
3. **Filter Assets** → Name starts with "CXR" AND extension is ".csv"
4. **Download Files** → From Monday.com public URLs
5. **Save to Data Lake** → `MondayBoards/input/files/csv/{boardID}/{itemID}-{assetID}.csv`
6. **Return Response** → Success with download count

## Technical Details

### Authentication
- **Method**: API Token in Authorization header
- **Format**: `Authorization: {api_token}`

### GraphQL Queries

**Board Data Query**:
```graphql
query { 
  boards (ids: {boardID}) { 
    items_page (limit: 500) { 
      items { 
        id 
        name 
        column_values { 
          id 
          value 
        } 
        assets { 
          id 
          name 
          url 
          public_url 
          file_extension 
        } 
      } 
    } 
  } 
}
```

**File Data Query**:
```graphql
query { 
  boards (ids: {boardID}) { 
    items_page (limit: 500) { 
      items { 
        id 
        name 
        assets { 
          id 
          name 
          url 
          public_url 
          file_extension 
        } 
      } 
    } 
  } 
}
```

### File Filtering Logic
```python
# CXR CSV Filter
if (asset_name and len(asset_name) >= 3 and 
    asset_name[:3].lower() == 'cxr' and 
    file_extension == '.csv' and 
    asset_url):
    # Download and save file
```

### Data Lake Storage
- **Account**: `prodbimanager`
- **Filesystem**: `prodbidlstorage`
- **JSON Path**: `MondayBoards/input/files/json/boards/`
- **CSV Path**: `MondayBoards/input/files/csv/{boardID}/`

### File Naming
- **JSON Files**: `{boardID}.json`
- **CSV Files**: `{itemID}-{assetID}.csv`

### Hardcoded Values
- **API Base URL**: `https://api.monday.com`
- **API Version**: `v2`
- **Item Limit**: 500 items per query
- **Storage Path**: `MondayBoards/input/files/`
- **File System**: `prodbidlstorage`

### Default Parameters
- **base_url**: `https://api.monday.com`
- **api_version**: `v2`
- **storage_path**: `MondayBoards/input/files/`
- **file_system**: `prodbidlstorage`

## How to Use

### 1. Get Board Metadata
```bash
curl -X POST "https://monday-downloader.azurewebsites.net/api/get_board_data?code=YOUR_FUNCTION_KEY_HERE" \
  -H "Content-Type: application/json" \
  -d '{
    "boardID": "4598613914",
    "api_token": "your_monday_api_token",
    "datalake_key": "your_azure_datalake_key"
  }'
```

### 2. Download CXR CSV Files
```bash
curl -X POST "https://monday-downloader.azurewebsites.net/api/get_file_data?code=YOUR_FUNCTION_KEY_HERE" \
  -H "Content-Type: application/json" \
  -d '{
    "boardID": "4598613914", 
    "api_token": "your_monday_api_token",
    "datalake_key": "your_azure_datalake_key"
  }'
```

### Function Keys
- **get_board_data**: `YOUR_GET_BOARD_DATA_FUNCTION_KEY`
- **get_file_data**: `YOUR_GET_FILE_DATA_FUNCTION_KEY`

## Error Handling

### Common Errors
- **Missing api_token**: "The supplied code is not right"
- **Missing datalake_key**: "Missing required parameter: datalake_key"
- **API Failure**: "Failed to fetch board data from Monday API"
- **Data Lake Failure**: "Failed to save to Data Lake"

### Debug Information
Functions include detailed logging for:
- API requests and responses
- Asset filtering results
- File download attempts
- Data Lake save operations

## Deployment

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Deploy to Azure Functions**
3. **Configure function keys** for authentication
4. **Test with valid Monday.com API token and Azure Data Lake key**
