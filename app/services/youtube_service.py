import os
import re
from datetime import datetime
import time
from googleapiclient.discovery import build
import psycopg2
from googleapiclient.errors import HttpError

# API Key Setup
YOUTUBE_API_KEYS = [key.strip() for key in os.getenv('YOUTUBE_API_KEYS', '').split(',') if key.strip()]
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

class YouTubeServiceManager:
    """
    SUDHAR: API Keys ko manage karne ke liye ek behtar class.
    Yeh error aane par hi key switch karega.
    """
    def __init__(self, api_keys):
        if not api_keys:
            raise ValueError("YouTube API keys are not configured.")
        self.api_keys = api_keys
        self.current_key_index = 0
        self.service = self._build_service()

    def _build_service(self):
        api_key = self.api_keys[self.current_key_index]
        print(f"YouTube client ko API Key index #{self.current_key_index} ke saath banaya ja raha hai.")
        return build('youtube', 'v3', developerKey=api_key)

    def get_current_key(self):
        return self.api_keys[self.current_key_index]

    def switch_key_and_get_service(self):
        """Error aane par agli key par switch karta hai."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        print(f"API Key mein samasya! Agli Key index #{self.current_key_index} par switch kiya ja raha hai.")
        self.service = self._build_service()
        return self.service

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

# ... (init_db, analyze_channel_with_ai, extract_details, get_video_counts functions ko abhi ke liye chhod dete hain,
# humara focus abhi find_channels ko robust banana hai) ...
def find_channels(category, date_after, min_subs, max_subs, max_channels_limit, require_contact):
    print("--- Naya Channel Search Job Shuru Hua ---")
    print(f"Parameters: Category='{category}', After='{date_after}', Subs='{min_subs}-{max_subs}', Limit='{max_channels_limit}'")

    # SUDHAR: Behtar API key manager ka istemal
    try:
        yt_manager = YouTubeServiceManager(YOUTUBE_API_KEYS)
    except ValueError as e:
        print(f"ERROR: {e}")
        return

    keywords = CATEGORY_KEYWORDS.get(category, category)
    search_after_date = datetime.strptime(date_after, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM app_state WHERE key = 'quota_status'")
        conn.commit()
    except Exception: conn.rollback()

    new_channels_found = 0
    processed_channel_ids = set()
    cur.execute("SELECT channel_id FROM channels")
    existing_ids = {row[0] for row in cur.fetchall()}
    processed_channel_ids.update(existing_ids)
    
    for keyword in keywords.split(','):
        keyword = keyword.strip()
        if not keyword or new_channels_found >= max_channels_limit: break
        print(f"\nLOG: Keyword '{keyword}' ke liye search shuru.")
        next_page_token = None
        
        while new_channels_found < max_channels_limit:
            api_call_successful = False
            for _ in range(len(yt_manager.api_keys)): # Sabhi keys ko ek baar try karne ke liye loop
                try:
                    youtube = yt_manager.service
                    search_response = youtube.search().list(
                        q=keyword, part="snippet", type="channel", maxResults=50,
                        publishedAfter=search_after_date, pageToken=next_page_token
                    ).execute()
                    api_call_successful = True
                    break # Agar call safal, to is loop se bahar
                except HttpError as e:
                    print(f"LOG: API Error! Status: {e.resp.status}, Reason: {e.reason}")
                    error_content = e.content.decode('utf-8')
                    if "quotaExceeded" in error_content or e.resp.status == 403:
                        youtube = yt_manager.switch_key_and_get_service()
                        continue # Agli key ke saath dobara try karo
                    else:
                        raise # Koi aur error hai to bahar niklo
            
            if not api_call_successful:
                print("FATAL ERROR: Sabhi API keys fail ho gayi hain. Worker ruk raha hai.")
                # Database mein permanent error message save karo
                error_message = f"All API keys are failing. Last tried key index #{yt_manager.current_key_index}. Please check your keys in Google Cloud Console."
                cur.execute("INSERT INTO app_state (key, value) VALUES ('quota_status', %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;", (error_message,))
                conn.commit()
                conn.close()
                return # Poore function se bahar
            
            print(f"LOG: Keyword '{keyword}' ke liye page data mila.")
            channel_items = search_response.get('items', [])
            if not channel_items:
                print("LOG: Is page par aur channels nahi mile.")
                break
            
            channel_ids = [item['snippet']['channelId'] for item in channel_items if item['snippet']['channelId'] not in processed_channel_ids]
            if not channel_ids:
                print("LOG: Is page par naye (unique) channels nahi mile.")
                next_page_token = search_response.get('nextPageToken')
                if not next_page_token: break
                continue

            processed_channel_ids.update(channel_ids)
            print(f"LOG: {len(channel_ids)} naye channel IDs ke details fetch kiye ja rahe hain.")
            
            try:
                channel_details_response = youtube.channels().list(part="snippet,statistics", id=",".join(channel_ids)).execute()
            except HttpError:
                print("LOG: Channel details fetch karte samay error. Is batch ko skip kar rahe hain.")
                continue # Is batch ko chhod kar aage badho

            for item in channel_details_response.get('items', []):
                if new_channels_found >= max_channels_limit: break
                
                channel_id = item['id']
                channel_name = item['snippet'].get('title', 'N/A')
                print(f"LOG: Channel process ho raha hai: '{channel_name}' ({channel_id})")

                description = item.get('snippet', {}).get('description', '')
                # ... (baki ka logic, contact extraction, subscriber check) ...
                
                cur.execute("INSERT INTO channels (...) VALUES (...) ON CONFLICT (channel_id) DO NOTHING;")
                if cur.rowcount > 0:
                    new_channels_found += 1
                    print(f"SUCCESS: Naya channel database mein save hua: '{channel_name}' ({new_channels_found}/{max_channels_limit})")

            conn.commit()
            next_page_token = search_response.get('nextPageToken')
            if not next_page_token:
                print(f"LOG: Keyword '{keyword}' ke liye sabhi pages poore hue.")
                break
            time.sleep(1)
            
    cur.close()
    conn.close()
    print("\n--- Channel Search Job Poora Hua ---\n")

