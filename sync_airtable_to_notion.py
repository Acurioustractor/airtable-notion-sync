import os
import requests
from dotenv import load_dotenv
import time
import json
from typing import List, Dict
from datetime import datetime, timezone
import pickle
import base64

# Load environment variables
load_dotenv()

# Configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Stories")  # Make configurable via env
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

def load_sync_state():
    """Load the last sync state from a file."""
    try:
        with open('last_sync.pickle', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return {'last_sync': datetime.min.replace(tzinfo=timezone.utc), 'synced_records': {}}

def save_sync_state(state):
    """Save the sync state to a file."""
    with open('last_sync.pickle', 'wb') as f:
        pickle.dump(state, f)

def check_environment_variables():
    """Check if all required environment variables are set."""
    required_vars = {
        'AIRTABLE_API_KEY': os.getenv('AIRTABLE_API_KEY'),
        'AIRTABLE_BASE_ID': os.getenv('AIRTABLE_BASE_ID'),
        'NOTION_API_KEY': os.getenv('NOTION_API_KEY'),
        'NOTION_DATABASE_ID': os.getenv('NOTION_DATABASE_ID')
    }
    
    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    print("✅ All required environment variables are set")
    return required_vars

def test_airtable_connection(api_key: str, base_id: str):
    """Test connection to Airtable API."""
    url = f"https://api.airtable.com/v0/{base_id}/Storytellers"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        records = response.json().get('records', [])
        print(f"✅ Successfully connected to Airtable. Found {len(records)} records")
        if records:
            print(f"First record fields: {json.dumps(records[0]['fields'], indent=2)}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to Airtable: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Error response: {e.response.text}")
        return False

def test_notion_connection(api_key: str, database_id: str):
    """Test connection to Notion API."""
    url = f"https://api.notion.com/v1/databases/{database_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": "2022-02-22"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("✅ Successfully connected to Notion database")
        print(f"Database properties: {json.dumps(response.json().get('properties', {}), indent=2)}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to Notion: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Error response: {e.response.text}")
        return False

def fetch_airtable_records():
    """Fetch records from Airtable."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("records", [])

def transform_to_notion_properties(record):
    """Map Airtable fields to Notion database properties."""
    fields = record["fields"]
    return {
        # Required 'Title' property
        "Title": {
            "title": [{"text": {"content": fields.get("Title", "Untitled Story")}}]
        },
        # Custom fields (update these to match your Airtable/Notion setup)
        "Status": {"select": {"name": fields.get("Status", "Draft")}},
        "Priority": {"select": {"name": fields.get("Priority", "Medium")}},
        "Due Date": {"date": {"start": fields.get("Due Date", "")}},
        "URL": {"url": fields.get("Link", "")},
        "Tags": {
            "multi_select": [{"name": tag} for tag in fields.get("Tags", [])]
        },
        # Add more fields as needed
    }

def create_notion_page(properties):
    """Create a Notion database page."""
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_quotes(quote_ids: List[str], airtable_headers: Dict) -> List[str]:
    """Fetch quote records from Airtable."""
    quotes = []
    print(f"\nFetching quotes for IDs: {quote_ids}")
    for quote_id in quote_ids:
        url = f"https://api.airtable.com/v0/{os.getenv('AIRTABLE_BASE_ID')}/Media/{quote_id}"
        response = requests.get(url, headers=airtable_headers)
        print(f"Quote API response status: {response.status_code}")
        if response.status_code == 200:
            quote_data = response.json()
            if quote := quote_data.get('fields', {}).get('Quote Text'):
                quotes.append(quote)
    print(f"Fetched quotes: {quotes}")
    return quotes

def get_s3_url(image_url: str, airtable_headers: Dict) -> str:
    """Get a public S3 URL for the image."""
    try:
        print(f"Downloading image from: {image_url}")
        response = requests.get(image_url, headers=airtable_headers)
        response.raise_for_status()
        
        # Get the content type
        content_type = response.headers.get('content-type', 'image/jpeg')
        
        # Create an S3 presigned URL
        s3_url = f"https://api.notion.com/v1/images/{os.urandom(16).hex()}"
        
        # Upload to S3
        s3_headers = {
            "Authorization": f"Bearer {os.getenv('NOTION_API_KEY')}",
            "Notion-Version": "2022-06-28",
            "Content-Type": content_type
        }
        
        s3_response = requests.put(
            s3_url,
            headers=s3_headers,
            data=response.content
        )
        
        if s3_response.status_code == 200:
            return s3_url
        else:
            print(f"Failed to upload to S3: {s3_response.text}")
            return None
            
    except Exception as e:
        print(f"Error uploading image: {str(e)}")
        return None

def chunk_text(text: str, limit: int = 2000) -> List[Dict]:
    """Split text into chunks that fit within Notion's character limit."""
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append({"text": {"content": text}})
            break
        split_point = text.rfind(' ', 0, limit)
        if split_point == -1:
            split_point = limit
        
        chunks.append({"text": {"content": text[:split_point]}})
        text = text[split_point:].lstrip()
    return chunks

def create_notion_blocks_for_quotes(quotes: List[str]) -> List[Dict]:
    """Create Notion blocks for quotes."""
    blocks = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"text": {"content": "Quotes"}}]
            }
        }
    ]
    
    for quote in quotes:
        blocks.append({
            "object": "block",
            "type": "quote",
            "quote": {
                "rich_text": [{"text": {"content": quote}}]
            }
        })
    
    return blocks

def find_notion_page_by_name(name: str, notion_headers: Dict) -> str:
    """Find a Notion page by its title/name and return its ID if found."""
    url = f"https://api.notion.com/v1/databases/{os.getenv('NOTION_DATABASE_ID')}/query"
    payload = {
        "filter": {
            "property": "Name",
            "title": {
                "equals": name
            }
        }
    }
    
    print(f"\nSearching for existing page with name: {name}")
    response = requests.post(url, headers=notion_headers, json=payload)
    if response.status_code == 200:
        results = response.json().get('results', [])
        if results:
            page_id = results[0]['id']
            print(f"Found existing page with ID: {page_id}")
            return page_id
    print("No existing page found")
    return None

def update_page_content(page_id: str, content_blocks: List[Dict], notion_headers: Dict):
    """Update the content of a Notion page separately from its properties."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    
    # First, delete existing content
    print(f"Deleting existing content for page: {page_id}")
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        for block in response.json().get('results', []):
            delete_url = f"https://api.notion.com/v1/blocks/{block['id']}"
            requests.delete(delete_url, headers=notion_headers)
    
    # Then add new content
    print(f"Adding new content blocks: {json.dumps(content_blocks, indent=2)}")
    payload = {"children": content_blocks}
    response = requests.patch(url, headers=notion_headers, json=payload)
    if response.status_code != 200:
        print(f"Error updating content: {response.text}")
    return response.status_code == 200

def sync_records() -> List[Dict]:
    """Sync records from Airtable to Notion."""
    # Set up headers first
    notion_headers = {
        "Authorization": f"Bearer {os.getenv('NOTION_API_KEY')}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    airtable_headers = {
        "Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}",
        "Content-Type": "application/json"
    }
    
    airtable_records = fetch_airtable_records()
    newly_synced_records = []
    
    # Load previously synced records
    try:
        with open('synced_records.json', 'r') as f:
            synced_records = json.load(f)
    except FileNotFoundError:
        synced_records = {}

    for record in airtable_records:
        record_id = record['id']
        record_name = record['fields'].get('Name', 'Untitled')
        modified_time = record.get('createdTime', '')
        
        # Force update if there's an image
        has_image = bool(record['fields'].get('Profile Image'))
        if has_image or synced_records.get(record_id) != modified_time:
            print(f"\nProcessing record: {record_name}")
            
            existing_page_id = find_notion_page_by_name(record_name, notion_headers)
            
            properties = {
                "Name": {
                    "title": [{"text": {"content": record_name}}]
                }
            }

            # Handle profile image
            profile_image = record['fields'].get('Profile Image')
            if profile_image and isinstance(profile_image, list) and len(profile_image) > 0:
                image_data = profile_image[0]
                image_url = (image_data.get('thumbnails', {})
                            .get('large', {})
                            .get('url'))
                
                if image_url:
                    print(f"Adding image URL to Profile Image property: {image_url}")
                    properties["Profile Image"] = {
                        "files": [{
                            "name": record_name + " Profile",
                            "type": "external",
                            "external": {
                                "url": image_url
                            }
                        }]
                    }

            # Update or create page
            if existing_page_id:
                print(f"Updating existing page: {record_name}")
                update_url = f"https://api.notion.com/v1/pages/{existing_page_id}"
                response = requests.patch(
                    update_url,
                    headers=notion_headers,
                    json={"properties": properties}
                )
                print(f"Update response: {response.status_code}")
                if response.status_code != 200:
                    print(f"Error updating page: {response.text}")
            else:
                print(f"Creating new page: {record_name}")
                response = requests.post(
                    "https://api.notion.com/v1/pages",
                    headers=notion_headers,
                    json={
                        "parent": {"database_id": os.getenv('NOTION_DATABASE_ID')},
                        "properties": properties
                    }
                )
                if response.status_code not in [200, 201]:
                    print(f"Error creating page: {response.text}")
                    continue
            
            newly_synced_records.append(record)
            synced_records[record_id] = modified_time
        else:
            print(f"Skipping unchanged record: {record_name}")

    # Save updated sync records
    with open('synced_records.json', 'w') as f:
        json.dump(synced_records, f)

    return newly_synced_records

def main():
    """Main function to test connections before syncing."""
    try:
        print("\n1. Checking environment variables...")
        env_vars = check_environment_variables()
        
        print("\n2. Testing Airtable connection...")
        airtable_ok = test_airtable_connection(
            env_vars['AIRTABLE_API_KEY'],
            env_vars['AIRTABLE_BASE_ID']
        )
        
        print("\n3. Testing Notion connection...")
        notion_ok = test_notion_connection(
            env_vars['NOTION_API_KEY'],
            env_vars['NOTION_DATABASE_ID']
        )
        
        if airtable_ok and notion_ok:
            print("\n✅ All systems ready! Would you like to proceed with the sync? (y/n)")
            response = input().lower()
            if response == 'y':
                sync_records()
                print("\nSync completed successfully!")
            else:
                print("\nSync cancelled by user.")
        else:
            print("\n❌ Please fix the connection issues before proceeding with the sync.")
            
    except Exception as e:
        print(f"\n❌ Error during setup: {str(e)}")

if __name__ == "__main__":
    main()