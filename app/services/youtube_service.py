import os
import re
from datetime import datetime
from googleapiclient.discovery import build
import psycopg2
import google.generativeai as genai
import math

# --- API Key Setup ---
YOUTUBE_API_KEYS = os.getenv('YOUTUBE_API_KEYS', '').split(',')
youtube_key_index = 0

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# --- Category to Keyword Mapping ---
CATEGORY_KEYWORDS = {
    # (Yahan wahi expanded keyword list paste karein jo pehle di gayi thi)
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
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY, channel_id VARCHAR(255) UNIQUE NOT NULL,
            channel_name TEXT NOT NULL, subscriber_count INTEGER,
            creation_date VARCHAR(100), emails TEXT, phone_numbers TEXT,
            description TEXT, retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            category VARCHAR(100), ai_summary TEXT, ai_tone VARCHAR(100), ai_audience TEXT,
            status VARCHAR(50) DEFAULT 'New',
            instagram_link TEXT, twitter_link TEXT, linkedin_link TEXT,
            short_videos_count INTEGER DEFAULT 0,
            long_videos_count INTEGER DEFAULT 0
        ); ''')
    # **NEW**: Jobs table for background processing
    cur.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            params JSONB NOT NULL,
            status VARCHAR(50) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        );
    ''')
    columns_to_add = {
        'instagram_link': 'TEXT', 'twitter_link': 'TEXT', 'linkedin_link': 'TEXT',
        'short_videos_count': 'INTEGER DEFAULT 0',
        'long_videos_count': 'INTEGER DEFAULT 0'
    }
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='channels';")
    existing_columns = [row[0] for row in cur.fetchall()]
    for col, col_type in columns_to_add.items():
        if col not in existing_columns:
            cur.execute(f"ALTER TABLE channels ADD COLUMN {col} {col_type};")
    conn.commit()
    cur.close()
    conn.close()

# Baki saare functions (get_youtube_service, analyze_channel_with_ai, etc.) yahan poore paste karein
# (Full code is identical to the previous 'youtube_logic.py (Full Update)' step)
# This is to ensure the file is complete.
def get_youtube_service():
    global youtube_key_index
    if not any(YOUTUBE_API_KEYS) or YOUTUBE_API_KEYS == ['']:
        raise ValueError("YouTube API keys are not configured.")
    api_key = YOUTUBE_API_KEYS[youtube_key_index]
    youtube_key_index = (youtube_key_index + 1) % len(YOUTUBE_API_KEYS)
    print(f"Using YouTube API Key index: {youtube_key_index}")
    return build('youtube', 'v3', developerKey=api_key)

def analyze_channel_with_ai(description):
    if not GEMINI_API_KEY: return "AI Not Configured", "N/A", "N/A"
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025')
        prompt = f"""Analyze the following YouTube channel description and provide a one-sentence summary, the primary tone (e.g., Professional, Casual, Funny, Educational), and the likely target audience (e.g., Students, Gamers, Professionals).
        Description: "{description}"
        Format your response exactly as follows:
        Summary: [Your one-sentence summary]
        Tone: [The primary tone]
        Audience: [The target audience]"""
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
    phones = list(set(re.findall(r'(?:\+91)?[ -]?[6-9]\d{9}', description)))
    instagram = re.search(r'(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)', description)
    twitter = re.search(r'(?:https?://)?(?:www\.)?twitter\.com/([a-zA-Z0-9_]+)', description)
    linkedin = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)', description)
    return {"emails": ", ".join(emails), "phones": ", ".join(phones), "instagram": instagram.group(0) if instagram else None, "twitter": twitter.group(0) if twitter else None, "linkedin": linkedin.group(0) if linkedin else None}

def get_video_counts(youtube_service, channel_id):
    try:
        channel_response = youtube_service.channels().list(part='contentDetails', id=channel_id).execute()
        if not channel_response.get('items'): return 0, 0
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        shorts_count, long_videos_count = 0, 0
        playlist_response = youtube_service.playlistItems().list(part='contentDetails', playlistId=uploads_playlist_id, maxResults=50).execute()
        for item in playlist_response.get('items', []):
            video_id = item['contentDetails']['videoId']
            video_details = youtube_service.videos().list(part='contentDetails', id=video_id).execute()
            if video_details.get('items'):
                duration_iso = video_details['items'][0]['contentDetails']['duration']
                total_seconds = 0
                if 'H' in duration_iso: total_seconds += int(re.search(r'(\d+)H', duration_iso).group(1)) * 3600
                if 'M' in duration_iso: total_seconds += int(re.search(r'(\d+)M', duration_iso).group(1)) * 60
                if 'S' in duration_iso: total_seconds += int(re.search(r'(\d+)S', duration_iso).group(1))
                if total_seconds <= 60: shorts_count += 1
                else: long_videos_count += 1
        return shorts_count, long_videos_count
    except Exception as e:
        print(f"Error fetching video counts for channel {channel_id}: {e}")
        return 0, 0

def find_channels(category, date_after, min_subs, max_subs, max_channels_limit, require_contact):
    keywords = CATEGORY_KEYWORDS.get(category, category)
    search_after_date = datetime.strptime(date_after, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
    conn = get_db_connection()
    cur = conn.cursor()
    new_channels_found = 0
    for keyword in keywords.split(','):
        keyword = keyword.strip()
        if not keyword or new_channels_found >= max_channels_limit: break
        try:
            youtube = get_youtube_service()
            next_page_token = None
            while True:
                if new_channels_found >= max_channels_limit: break
                search_request = youtube.search().list(q=keyword, part="snippet", type="channel", maxResults=50, publishedAfter=search_after_date, pageToken=next_page_token)
                search_response = search_request.execute()
                channel_ids_from_search_page = [item['snippet']['channelId'] for item in search_response.get('items', [])]
                if not channel_ids_from_search_page: break
                cur.execute("SELECT channel_id FROM channels WHERE channel_id = ANY(%s)", (channel_ids_from_search_page,))
                existing_ids = {row[0] for row in cur.fetchall()}
                new_ids_to_fetch_details = [cid for cid in channel_ids_from_search_page if cid not in existing_ids]
                if not new_ids_to_fetch_details:
                    next_page_token = search_response.get('nextPageToken')
                    if not next_page_token: break
                    continue
                channel_details_request = youtube.channels().list(part="snippet,statistics", id=",".join(new_ids_to_fetch_details))
                channel_details_response = channel_details_request.execute()
                for item in channel_details_response.get('items', []):
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
                            cur.execute(""" INSERT INTO channels (channel_id, channel_name, subscriber_count, creation_date, emails, phone_numbers, description, category, ai_summary, ai_tone, ai_audience, instagram_link, twitter_link, linkedin_link, short_videos_count, long_videos_count) VALUES (%(channel_id)s, %(channel_name)s, %(subscriber_count)s, %(creation_date)s, %(emails)s, %(phone_numbers)s, %(description)s, %(category)s, %(ai_summary)s, %(ai_tone)s, %(ai_audience)s, %(instagram_link)s, %(twitter_link)s, %(linkedin_link)s, %(short_videos_count)s, %(long_videos_count)s) ON CONFLICT (channel_id) DO UPDATE SET channel_name = EXCLUDED.channel_name, subscriber_count = EXCLUDED.subscriber_count, description = EXCLUDED.description, emails = EXCLUDED.emails, phone_numbers = EXCLUDED.phone_numbers, instagram_link = EXCLUDED.instagram_link, twitter_link = EXCLUDED.twitter_link, linkedin_link = EXCLUDED.linkedin_link, short_videos_count = EXCLUDED.short_videos_count, long_videos_count = EXCLUDED.long_videos_count, retrieved_at = CURRENT_TIMESTAMP; """, channel_info)
                            new_channels_found += 1
                conn.commit()
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break
        except Exception as e:
            print(f"Ek error aayi '{keyword}' keyword ke saath: {e}")
            if 'quotaExceeded' in str(e).lower(): break
    cur.close()
    conn.close()
