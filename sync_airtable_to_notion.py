import os
import requests
import json
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

def get_all_quotes(headers, base_id):
    """Fetch all quotes from Airtable at once."""
    print("Fetching all quotes...")
    quotes_url = f"https://api.airtable.com/v0/{base_id}/Quotes"
    response = requests.get(quotes_url, headers=headers)
    
    if response.status_code == 200:
        quotes_data = response.json().get('records', [])
        quotes_dict = {q['id']: q['fields'].get('Quote Text', '') for q in quotes_data}
        print(f"Found {len(quotes_dict)} quotes")
        return quotes_dict
    return {}

def get_existing_pages(headers, database_id):
    """Get existing pages from Notion database."""
    print("Fetching existing pages...")
    existing_pages = {}
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    
    while True:
        response = requests.post(url, headers=headers, json={})
        if response.status_code != 200:
            print(f"Error fetching pages: {response.text}")
            break
            
        data = response.json()
        for page in data.get('results', []):
            title_property = page['properties']['Name']['title']
            if title_property:
                name = title_property[0]['text']['content']
                existing_pages[name] = page['id']
                print(f"Found existing page: {name}")
            
        if not data.get('has_more'):
            break
            
    print(f"Found {len(existing_pages)} existing pages")
    return existing_pages

def chunk_text(text, limit=2000):
    """Split text into chunks of maximum size."""
    return [text[i:i+limit] for i in range(0, len(text), limit)]

print("Starting sync process...")

# Get quotes lookup table
quotes_lookup = get_all_quotes(airtable_headers, os.getenv('AIRTABLE_BASE_ID'))

# Get existing pages
existing_pages = get_existing_pages(notion_headers, os.getenv('NOTION_DATABASE_ID'))

# Get Airtable records
airtable_url = f"https://api.airtable.com/v0/{os.getenv('AIRTABLE_BASE_ID')}/Storytellers"
response = requests.get(airtable_url, headers=airtable_headers)
records = response.json().get('records', [])

print(f"Processing {len(records)} records...")

# Process each record
for record in records:
    fields = record['fields']
    
    if 'Name' not in fields:
        print("Skipping record without name")
        continue
        
    name = fields['Name']
    print(f"\nProcessing: {name}")
    
    # Skip if page already exists
    if name in existing_pages:
        print(f"Page already exists for {name}, skipping...")
        continue

    # Prepare properties
    properties = {
        "Name": {"title": [{"text": {"content": name}}]},
    }

    # Add select properties only if they have values
    if location := fields.get('Location'):
        properties["Location"] = {"select": {"name": location}}
    
    if org := fields.get('Organisation'):
        properties["Organisation"] = {"select": {"name": org}}
    
    if project := fields.get('Project'):
        properties["Project"] = {"select": {"name": project}}
    
    if prefs := fields.get('Preferences'):
        properties["Preferences"] = {"select": {"name": prefs}}
    
    # Add date if it exists
    if created_at := fields.get('Created At'):
        properties["Created At"] = {"date": {"start": created_at}}

    # Add Summary to properties
    if summary := fields.get('Summary (from Media)'):
        summary_text = summary[0] if isinstance(summary, list) else summary
        properties["Summary"] = {
            "rich_text": [{"text": {"content": summary_text}}]
        }

    # Add Description to properties
    if descriptions := fields.get('Description (from Themes) (from Media)'):
        if isinstance(descriptions, list):
            description_text = "\n\n".join(descriptions)
        else:
            description_text = descriptions
        properties["Description"] = {
            "rich_text": [{"text": {"content": description_text}}]
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

    # Create the page
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
            print(f"Created page for: {name}")
            page_id = response.json()['id']
            
            # Prepare the content blocks
            blocks = []
            
            # Add Quotes section if exists
            if quote_ids := fields.get('Quotes (from Media)'):
                if isinstance(quote_ids, list) and quotes_lookup:
                    blocks.extend([
                        {
                            "object": "block",
                            "type": "heading_1",
                            "heading_1": {
                                "rich_text": [{"type": "text", "text": {"content": "Key Quotes"}}]
                            }
                        }
                    ])
                    
                    for quote_id in quote_ids:
                        if quote_text := quotes_lookup.get(quote_id):
                            blocks.append({
                                "object": "block",
                                "type": "quote",
                                "quote": {
                                    "rich_text": [{"type": "text", "text": {"content": quote_text}}]
                                }
                            })
                    
                    blocks.append({
                        "object": "block",
                        "type": "divider",
                        "divider": {}
                    })
            
            # Add Transcript section if exists
            if transcript := fields.get('Transcript (from Media)'):
                blocks.extend([
                    {
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": [{"type": "text", "text": {"content": "Full Transcript"}}]
                        }
                    }
                ])
                
                transcript_text = transcript[0] if isinstance(transcript, list) else transcript
                for chunk in chunk_text(transcript_text):
                    blocks.append({
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": chunk}}]
                        }
                    })
            
            # Add the blocks to the page
            if blocks:
                blocks_response = requests.patch(
                    f"https://api.notion.com/v1/blocks/{page_id}/children",
                    headers=notion_headers,
                    json={"children": blocks}
                )
                
                if blocks_response.status_code in [200, 201]:
                    print(f"Added content blocks for: {name}")
                else:
                    print(f"Error adding blocks for {name}: {blocks_response.text}")
            
        else:
            print(f"Error with {name}: {response.text}")
            
    except Exception as e:
        print(f"Error processing {name}: {str(e)}")

print("\nSync completed!")