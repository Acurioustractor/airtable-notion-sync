import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up headers
notion_headers = {
    "Authorization": f"Bearer {os.getenv('NOTION_API_KEY')}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

airtable_headers = {
    "Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}",
    "Content-Type": "application/json"
}

print("Starting sync...")

# Get Airtable records
airtable_url = f"https://api.airtable.com/v0/{os.getenv('AIRTABLE_BASE_ID')}/Media"
response = requests.get(airtable_url, headers=airtable_headers)
records = response.json().get('records', [])

print(f"Processing {len(records)} records...")

# Process each record
for record in records:
    fields = record['fields']
    name = fields.get('Name', 'Untitled')
    print(f"Processing: {name}")
    
    # Prepare properties
    properties = {
        "Name": {"title": [{"text": {"content": name}}]},
        "Location": {"select": {"name": fields.get('Location', '')}},
        "Organisation": {"select": {"name": fields.get('Organisation', '')}},
        "Project": {"select": {"name": fields.get('Project', '')}},
        "Preferences": {"select": {"name": fields.get('Preferences', '')}},
        "Created At": {"date": {"start": fields.get('Created At', '')}},
    }

    # Handle Summary
    if summary := fields.get('Summary (from Media)'):
        if isinstance(summary, list):
            summary = summary[0]
        properties["Summary"] = {
            "rich_text": [{"text": {"content": summary}}]
        }

    # Handle Description
    if descriptions := fields.get('Description (from Themes) (from Media)'):
        if isinstance(descriptions, list):
            description = "\n\n".join(descriptions)
        else:
            description = descriptions
        properties["Description"] = {
            "rich_text": [{"text": {"content": description}}]
        }

    # Handle profile image
    if profile_images := fields.get('Profile Image'):
        if isinstance(profile_images, list) and len(profile_images) > 0:
            if image_url := profile_images[0].get('thumbnails', {}).get('large', {}).get('url'):
                properties["Profile Image"] = {
                    "files": [{
                        "name": f"{name} Profile",
                        "type": "external",
                        "external": {"url": image_url}
                    }]
                }

    # Create in Notion
    try:
        response = requests.post(
            f"https://api.notion.com/v1/pages",
            headers=notion_headers,
            json={
                "parent": {"database_id": os.getenv('NOTION_DATABASE_ID')},
                "properties": properties
            }
        )
        
        if response.status_code in [200, 201]:
            print(f"Successfully processed: {name}")
        else:
            print(f"Error with {name}: {response.text}")
            
    except Exception as e:
        print(f"Error processing {name}: {str(e)}")

print("Sync completed!")