import os
import re
from datetime import datetime
import time
from googleapiclient.discovery import build
import psycopg2
import google.generativeai as genai
from googleapiclient.errors import HttpError
# Email notification service ko hata diya gaya hai

# API Key Setup
YOUTUBE_API_KEYS = os.getenv('YOUTUBE_API_KEYS', '').split(',')
youtube_key_index = 0
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# Category Keywords (koi badlav nahi)
CATEGORY_KEYWORDS = {
    "Technology": "hindi tech, gadgets review, unboxing, mobile review, laptop review, tech news india, smartphone tips, android tricks, iphone tricks, programming hindi, python hindi, pc build india, latest gadgets, ai explained hindi, software development, cyber security awareness, tech tips and tricks, saste gadgets, tech channel",
    "Gaming": "gaming india, live gameplay, mobile gaming, pc games, bgmi live, valorant india, free fire gameplay, gta v hindi, minecraft hindi, gaming shorts, gaming channel, best android games, gaming pc build, ps5 india, pro gamer, gaming highlights, game walkthrough hindi, op gameplay",
    "Finance": "stock market india, personal finance, investing for beginners, mutual funds sahi hai, share market live, sip investment, cryptocurrency india, bitcoin hindi, how to save money, credit card tips, budgeting tips hindi, business ideas, startup india, case study hindi, make money online, nifty 50, intraday trading",
    "Education": "educational channel, study iq, online learning india, upsc preparation, ssc cgl, neet motivation, jee mains, current affairs 2025, gk in hindi, skill development, english speaking course, communication skills, history in hindi, science experiments, amazing facts, knowledge video, class 12",
    "Comedy": "hindi comedy, funny video, vines, stand up comedy, comedy sketch, funny roast, prank video india, desi comedy, animation comedy, mimicry, funny dubbing, bhojpuri comedy, haryanvi comedy, comedy shorts",
    "Vlogging": "daily vlog, lifestyle vlog, travel vlogger india, india travel vlog, mountain vlog, goa vlog, budget travel, moto vlogging india, food vlog, shopping haul, village life vlog, family vlog, couple vlog, a day in my life, my first vlog",
    "YouTube related": "youtube growth tips, how to grow youtube channel, youtube seo, 1000 subscribers kaise badhaye, 4000 watch time kaise kare, youtube studio tutorial, vidiq tutorial, tubebuddy tutorial, youtube policies hindi, monetize youtube channel, youtube earning tips, youtube shorts strategy, video editing for youtube, thumbnail tutorial, digital marketing for creators, content creation tips"
}

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def init_db():
    # SUDHAR: app_state table banane ke liye command joda gaya
    conn = get_db_connection()
    cur = conn.cursor()
    # Channels table (existing)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        id SERIAL PRIMARY KEY, channel_id VARCHAR(255) UNIQUE NOT NULL, channel_name TEXT NOT NULL,
        subscriber_count INTEGER, creation_date VARCHAR(100), emails TEXT, phone_numbers TEXT,
        description TEXT, retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, category VARCHAR(100),
        ai_summary TEXT, ai_tone VARCHAR(100), ai_audience TEXT, status VARCHAR(50) DEFAULT 'New',
        instagram_link TEXT, twitter_link TEXT, linkedin_link TEXT,
        short_videos_count INTEGER DEFAULT 0, long_videos_count INTEGER DEFAULT 0
    );''')
    # Jobs table (existing)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id SERIAL PRIMARY KEY, params JSONB NOT NULL, status VARCHAR(50) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP
    );''')
    # SUDHAR: Notification ke liye nayi table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS app_state (
        key VARCHAR(50) PRIMARY KEY,
        value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );''')
    conn.commit()
    cur.close()
    conn.close()
    print("Database tables checked/created.")


def get_youtube_service():
    global youtube_key_index
    if not any(YOUTUBE_API_KEYS) or YOUTUBE_API_KEYS == ['']:
        raise ValueError("YouTube API keys are not configured.")
    
    api_key = YOUTUBE_API_KEYS[youtube_key_index]
    print(f"Using API Key index: {youtube_key_index}")
    youtube_key_index = (youtube_key_index + 1) % len(YOUTUBE_API_KEYS)
    service = build('youtube', 'v3', developerKey=api_key)
    # SUDHAR: Ab hum service ke saath-saath key bhi return karenge
    return service, api_key

# Baki ke functions (analyze_channel_with_ai, extract_details, get_video_counts) waise hi rahenge...
def analyze_channel_with_ai(description):
    if not GEMINI_API_KEY: return "AI Not Configured", "N/A", "N/A"
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""Analyze the following YouTube channel description... Format: Summary: ... Tone: ... Audience: ..."""
        response = model.generate_content(prompt)
        text = response.text
        summary = re.search(r"Summary:\s*(.*)", text, re.IGNORECASE)
        tone = re.search(r"Tone:\s*(.*)", text, re.IGNORECASE)
        audience = re.search(r"Audience:\s*(.*)", text, re.IGNORECASE)
        return (summary.group(1).strip() if summary else "Could not determine"), (tone.group(1).strip() if tone else "N/A"), (audience.group(1).strip() if audience else "N/A")
    except Exception as e:
        print(f"AI analysis failed: {e}")
        return "AI analysis failed.", "N/A", "N/A"

def extract_details(description):
    emails = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', description)))
    phones = list(set(re.findall(r'(?:\+91)?[ -]?(?:[6-9]\d{2}[ -]?\d{3}[ -]?\d{4}|\d{10})', description)))
    instagram = re.search(r'(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9._]+)', description)
    twitter = re.search(r'(?:https?://)?(?:www\.)?twitter\.com/([a-zA-Z0-9_]+)', description)
    linkedin = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)', description)
    return {"emails": ", ".join(emails), "phones": ", ".join(phones), "instagram": instagram.group(0) if instagram else None, "twitter": twitter.group(0) if twitter else None, "linkedin": linkedin.group(0) if linkedin else None}

def get_video_counts(youtube_service, channel_id):
    try:
        shorts_request = youtube_service.search().list(part="id", channelId=channel_id, type="video", videoDuration="short", maxResults=1).execute()
        shorts_count = shorts_request.get('pageInfo', {}).get('totalResults', 0)
        long_videos_request = youtube_service.search().list(part="id", channelId=channel_id, type="video", videoDuration="long", maxResults=1).execute()
        long_videos_count = long_videos_request.get('pageInfo', {}).get('totalResults', 0)
        return shorts_count, long_videos_count
    except Exception as e:
        if isinstance(e, HttpError) and e.resp.status == 403: raise
        print(f"Error fetching video counts for channel {channel_id}: {e}")
        return 0, 0

def find_channels(category, date_after, min_subs, max_subs, max_channels_limit, require_contact):
    keywords = CATEGORY_KEYWORDS.get(category, category)
    search_after_date = datetime.strptime(date_after, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
    
    conn = get_db_connection()
    cur = conn.cursor()

    # SUDHAR: Naya kaam shuru hone par purana quota message hata dein
    try:
        cur.execute("DELETE FROM app_state WHERE key = 'quota_status'")
        conn.commit()
        print("Purana quota status message (agar tha to) clear kar diya gaya.")
    except Exception as db_err:
        print(f"Purana quota status clear karne mein error: {db_err}")
        conn.rollback()

    new_channels_found = 0
    processed_channel_ids = set()
    
    cur.execute("SELECT channel_id FROM channels")
    existing_ids = {row[0] for row in cur.fetchall()}
    processed_channel_ids.update(existing_ids)

    youtube, current_api_key = get_youtube_service()

    for keyword in keywords.split(','):
        keyword = keyword.strip()
        if not keyword or new_channels_found >= max_channels_limit:
            break
        
        print(f"'{keyword}' keyword ke liye search kiya ja raha hai...")
        next_page_token = None
        
        while new_channels_found < max_channels_limit:
            try:
                # ... baki search logic waisa hi rahega ...
                search_request = youtube.search().list(q=keyword, part="snippet", type="channel", maxResults=50, publishedAfter=search_after_date, pageToken=next_page_token)
                search_response = search_request.execute()
                channel_ids_from_search = [item['snippet']['channelId'] for item in search_response.get('items', [])]
                new_unique_ids = [cid for cid in channel_ids_from_search if cid not in processed_channel_ids]
                if not new_unique_ids: break
                processed_channel_ids.update(new_unique_ids)
                channel_details_request = youtube.channels().list(part="snippet,statistics", id=",".join(new_unique_ids))
                channel_details_response = channel_details_request.execute()

                for item in channel_details_response.get('items', []):
                    # ... baki channel processing logic waisa hi rahega ...
                    if new_channels_found >= max_channels_limit: break
                    description = item.get('snippet', {}).get('description', '')
                    details = extract_details(description)
                    if require_contact and not (details['emails'] or details['phones']): continue
                    stats = item.get('statistics', {})
                    if not stats.get('hiddenSubscriberCount', False):
                        subscriber_count = int(stats.get('subscriberCount', 0))
                        if min_subs <= subscriber_count <= max_subs:
                            ai_summary, ai_tone, ai_audience = analyze_channel_with_ai(description[:5000])
                            shorts_count, long_videos_count = get_video_counts(youtube, item['id'])
                            channel_info = { "channel_id": item['id'], "channel_name": item['snippet'].get('title'), "subscriber_count": subscriber_count, "creation_date": item['snippet'].get('publishedAt', '')[:10], "emails": details['emails'], "phone_numbers": details['phones'], "description": description, "category": category, "ai_summary": ai_summary, "ai_tone": ai_tone, "ai_audience": ai_audience, "instagram_link": details['instagram'], "twitter_link": details['twitter'], "linkedin_link": details['linkedin'], "short_videos_count": shorts_count, "long_videos_count": long_videos_count }
                            cur.execute(""" INSERT INTO channels (channel_id, channel_name, subscriber_count, creation_date, emails, phone_numbers, description, category, ai_summary, ai_tone, ai_audience, instagram_link, twitter_link, linkedin_link, short_videos_count, long_videos_count) VALUES (%(channel_id)s, %(channel_name)s, %(subscriber_count)s, %(creation_date)s, %(emails)s, %(phone_numbers)s, %(description)s, %(category)s, %(ai_summary)s, %(ai_tone)s, %(ai_audience)s, %(instagram_link)s, %(twitter_link)s, %(linkedin_link)s, %(short_videos_count)s, %(long_videos_count)s) ON CONFLICT (channel_id) DO NOTHING; """, channel_info)
                            if cur.rowcount > 0:
                                new_channels_found += 1
                                print(f"Naya channel mila: {channel_info['channel_name']} ({new_channels_found}/{max_channels_limit})")
                
                conn.commit()
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break
                time.sleep(1)

            except HttpError as e:
                error_content = e.content.decode('utf-8') if e.content else ''
                reason = "unknown"
                if "quotaExceeded" in error_content:
                    reason = "quotaExceeded"

                print(f"Ek HTTP error aayi '{keyword}' ke saath: {e}")
                if reason == 'quotaExceeded':
                    print("API Quota khatm ho gaya. Database mein status update kiya ja raha hai...")
                    
                    # SUDHAR: Yahan par database mein message save karenge
                    try:
                        # API key ke aakhiri 4 anko ko dikhane ke liye format karein
                        masked_key = f"....**{current_api_key[-4:]}"
                        error_message = f"Quota exhausted for key {masked_key} on {datetime.now().strftime('%Y-%m-%d %H:%M')}. Searches are paused until the quota resets."
                        
                        # Database mein save karein
                        cur.execute("""
                            INSERT INTO app_state (key, value, updated_at)
                            VALUES ('quota_status', %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (key) DO UPDATE
                            SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP;
                        """, (error_message,))
                        conn.commit()
                        print("Quota status message database mein save kar diya gaya.")

                    except Exception as db_err:
                        print(f"Quota status save karne mein error: {db_err}")
                        conn.rollback()

                    print("Worker ruk raha hai.")
                    conn.close()
                    return
                
                youtube, current_api_key = get_youtube_service() # Key badlo
                time.sleep(5)
            except Exception as e:
                print(f"Ek anjaan error aayi '{keyword}' keyword ke saath: {e}")
                time.sleep(10)
                break
    
    cur.close()
    conn.close()
    print("Search poora hua.")

