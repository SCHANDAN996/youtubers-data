# youtube_logic.py
import os
import re
from datetime import datetime, timedelta
from googleapiclient.discovery import build
import psycopg2

API_KEYS = os.getenv('YOUTUBE_API_KEYS', '').split(',')
key_index = 0

def get_youtube_service():
    global key_index
    if not any(API_KEYS) or API_KEYS == ['']:
        raise ValueError("YouTube API keys are not configured.")
    api_key = API_KEYS[key_index]
    key_index = (key_index + 1) % len(API_KEYS)
    print(f"Using API Key index: {key_index}")
    return build('youtube', 'v3', developerKey=api_key)

def get_db_connection():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            channel_id VARCHAR(255) UNIQUE NOT NULL,
            channel_name TEXT NOT NULL,
            subscriber_count INTEGER,
            creation_date VARCHAR(100),
            emails TEXT,
            phone_numbers TEXT,
            description TEXT,
            retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def extract_contact_details(description):
    emails = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', description)))
    phones = list(set(re.findall(r'(?:\+91)?[ -]?[6-9]\d{9}', description)))
    return ", ".join(emails), ", ".join(phones)

def find_channels(keywords, min_subs, max_subs, months_ago):
    search_after_date = (datetime.utcnow() - timedelta(days=months_ago * 30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    conn = get_db_connection()
    cur = conn.cursor()

    for keyword in keywords.split(','):
        keyword = keyword.strip()
        print(f"Searching for keyword: {keyword}")
        try:
            youtube = get_youtube_service()
            next_page_token = None
            page_count = 0
            while True and page_count < 5: # Safety break after 5 pages per keyword
                search_request = youtube.search().list(q=keyword, part="snippet", type="channel", maxResults=50, publishedAfter=search_after_date, pageToken=next_page_token)
                search_response = search_request.execute()
                channel_ids = [item['snippet']['channelId'] for item in search_response.get('items', [])]

                if not channel_ids: break

                channel_details_request = get_youtube_service().channels().list(part="snippet,statistics", id=",".join(channel_ids))
                channel_details_response = channel_details_request.execute()

                for item in channel_details_response.get('items', []):
                    stats = item.get('statistics', {})
                    if not stats.get('hiddenSubscriberCount', False):
                        subscriber_count = int(stats.get('subscriberCount', 0))
                        if min_subs <= subscriber_count <= max_subs:
                            snippet = item.get('snippet', {})
                            description = snippet.get('description', '')
                            emails, phones = extract_contact_details(description)
                            channel_info = {
                                "channel_id": item['id'], "channel_name": snippet.get('title'),
                                "subscriber_count": subscriber_count, "creation_date": snippet.get('publishedAt', '')[:10],
                                "emails": emails, "phone_numbers": phones, "description": description
                            }
                            cur.execute(
                                """
                                INSERT INTO channels (channel_id, channel_name, subscriber_count, creation_date, emails, phone_numbers, description)
                                VALUES (%(channel_id)s, %(channel_name)s, %(subscriber_count)s, %(creation_date)s, %(emails)s, %(phone_numbers)s, %(description)s)
                                ON CONFLICT (channel_id) DO NOTHING;
                                """,
                                channel_info
                            )
                conn.commit()
                next_page_token = search_response.get('nextPageToken')
                page_count += 1
                if not next_page_token: break
        except Exception as e:
            print(f"An error occurred with keyword '{keyword}': {e}")
            if 'quotaExceeded' in str(e).lower(): print("Quota exceeded, trying next key or stopping.")
    cur.close()
    conn.close()
