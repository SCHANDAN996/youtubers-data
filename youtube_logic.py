import os
import re
from datetime import datetime
from googleapiclient.discovery import build
import psycopg2
import google.generativeai as genai

# --- API Key Setup ---
YOUTUBE_API_KEYS = os.getenv('YOUTUBE_API_KEYS', '').split(',')
youtube_key_index = 0

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# --- Category to Keyword Mapping ---
CATEGORY_KEYWORDS = {
    "Technology": "hindi tech, gadgets, unboxing, programming, tech news",
    "Gaming": "gaming india, live gameplay, mobile gaming, pc games",
    "Finance": "stock market india, personal finance, investing, mutual funds",
    "Education": "educational channel, study iq, online learning india",
    "Comedy": "hindi comedy, funny video, vines, stand up comedy",
    "Vlogging": "daily vlog, lifestyle vlog, travel vlogger india"
}

def get_youtube_service():
    global youtube_key_index
    if not any(YOUTUBE_API_KEYS) or YOUTUBE_API_KEYS == ['']:
        raise ValueError("YouTube API keys are not configured.")
    api_key = YOUTUBE_API_KEYS[youtube_key_index]
    youtube_key_index = (youtube_key_index + 1) % len(YOUTUBE_API_KEYS)
    print(f"Using YouTube API Key index: {youtube_key_index}")
    return build('youtube', 'v3', developerKey=api_key)

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    # Adding new columns for AI Analysis and a status column
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
            retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            category VARCHAR(100),
            ai_summary TEXT,
            ai_tone VARCHAR(100),
            ai_audience TEXT,
            status VARCHAR(50) DEFAULT 'New' 
        );
    ''')
    # Add a function to safely add columns if they don't exist
    columns_to_add = {
        'category': 'VARCHAR(100)', 'ai_summary': 'TEXT', 'ai_tone': 'VARCHAR(100)',
        'ai_audience': 'TEXT', 'status': "VARCHAR(50) DEFAULT 'New'"
    }
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='channels';")
    existing_columns = [row[0] for row in cur.fetchall()]
    for col, col_type in columns_to_add.items():
        if col not in existing_columns:
            cur.execute(f"ALTER TABLE channels ADD COLUMN {col} {col_type};")

    conn.commit()
    cur.close()
    conn.close()

def analyze_channel_with_ai(description):
    if not GEMINI_API_KEY:
        return "AI Not Configured", "N/A", "N/A"
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025')
        prompt = f"""Analyze the following YouTube channel description and provide a one-sentence summary, the primary tone (e.g., Professional, Casual, Funny, Educational), and the likely target audience (e.g., Students, Gamers, Professionals).
        
        Description: "{description}"
        
        Format your response exactly as follows, with each item on a new line:
        Summary: [Your one-sentence summary]
        Tone: [The primary tone]
        Audience: [The target audience]
        """
        response = model.generate_content(prompt)
        
        # **IMPROVEMENT**: Robust parsing of AI response
        text = response.text
        summary_match = re.search(r"Summary: (.*)", text, re.IGNORECASE)
        tone_match = re.search(r"Tone: (.*)", text, re.IGNORECASE)
        audience_match = re.search(r"Audience: (.*)", text, re.IGNORECASE)
        
        summary = summary_match.group(1).strip() if summary_match else "Could not determine"
        tone = tone_match.group(1).strip() if tone_match else "N/A"
        audience = audience_match.group(1).strip() if audience_match else "N/A"
        
        return summary, tone, audience
    except Exception as e:
        print(f"AI analysis failed: {e}")
        return "AI analysis failed.", "N/A", "N/A"

def extract_contact_details(description):
    emails = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', description)))
    phones = list(set(re.findall(r'(?:\+91)?[ -]?[6-9]\d{9}', description)))
    return ", ".join(emails), ", ".join(phones)

def find_channels(category, date_after, min_subs, max_subs, max_channels_limit):
    keywords = CATEGORY_KEYWORDS.get(category, category)
    search_after_date = datetime.strptime(date_after, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
    
    conn = get_db_connection()
    cur = conn.cursor()
    new_channels_found = 0

    for keyword in keywords.split(','):
        if new_channels_found >= max_channels_limit: break
        keyword = keyword.strip()
        print(f"Searching for keyword: {keyword}")
        try:
            youtube = get_youtube_service()
            next_page_token = None
            while True:
                if new_channels_found >= max_channels_limit: break
                
                search_request = youtube.search().list(q=keyword, part="snippet", type="channel", maxResults=50, publishedAfter=search_after_date, pageToken=next_page_token)
                search_response = search_request.execute()
                
                channel_ids_from_search = [item['snippet']['channelId'] for item in search_response.get('items', [])]
                if not channel_ids_from_search: break

                # We can fetch all details and decide to INSERT or UPDATE later
                channel_details_request = get_youtube_service().channels().list(part="snippet,statistics", id=",".join(channel_ids_from_search))
                channel_details_response = channel_details_request.execute()

                for item in channel_details_response.get('items', []):
                    # Check if we should stop before processing
                    cur.execute("SELECT EXISTS(SELECT 1 FROM channels WHERE channel_id=%s)", (item['id'],))
                    is_existing = cur.fetchone()[0]
                    if is_existing:
                        # **IMPROVEMENT**: Logic to update existing record
                        stats = item.get('statistics', {})
                        subscriber_count = int(stats.get('subscriberCount', 0))
                        cur.execute(
                            """
                            UPDATE channels SET subscriber_count = %s, retrieved_at = CURRENT_TIMESTAMP 
                            WHERE channel_id = %s
                            """,
                            (subscriber_count, item['id'])
                        )
                        print(f"Updated channel: {item['snippet']['title']}")
                        continue # Move to the next item

                    if new_channels_found >= max_channels_limit: break

                    stats = item.get('statistics', {})
                    if not stats.get('hiddenSubscriberCount', False):
                        subscriber_count = int(stats.get('subscriberCount', 0))
                        if min_subs <= subscriber_count <= max_subs:
                            snippet = item.get('snippet', {})
                            description = snippet.get('description', '')
                            emails, phones = extract_contact_details(description)
                            
                            ai_summary, ai_tone, ai_audience = analyze_channel_with_ai(description[:5000])

                            channel_info = {
                                "channel_id": item['id'], "channel_name": snippet.get('title'),
                                "subscriber_count": subscriber_count, "creation_date": snippet.get('publishedAt', '')[:10],
                                "emails": emails, "phone_numbers": phones, "description": description, "category": category,
                                "ai_summary": ai_summary, "ai_tone": ai_tone, "ai_audience": ai_audience
                            }
                            # This now only runs for brand new channels
                            cur.execute(
                                """
                                INSERT INTO channels (channel_id, channel_name, subscriber_count, creation_date, emails, phone_numbers, description, category, ai_summary, ai_tone, ai_audience)
                                VALUES (%(channel_id)s, %(channel_name)s, %(subscriber_count)s, %(creation_date)s, %(emails)s, %(phone_numbers)s, %(description)s, %(category)s, %(ai_summary)s, %(ai_tone)s, %(ai_audience)s)
                                """,
                                channel_info
                            )
                            new_channels_found += 1
                
                conn.commit() # Commit after each page of results
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break
        
        except Exception as e:
            print(f"An error occurred: {e}")
            if 'quotaExceeded' in str(e).lower():
                print("YouTube API quota likely exceeded. Stopping search.")
                break
    
    cur.close()
    conn.close()
