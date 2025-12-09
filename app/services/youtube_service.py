import os
import re
from datetime import datetime
import time
from googleapiclient.discovery import build
import psycopg2
from googleapiclient.errors import HttpError

# API Keys ko saaf karke list banayein, khali entries ko hata dein
YOUTUBE_API_KEYS = [key.strip() for key in os.getenv('YOUTUBE_API_KEYS', '').split(',') if key.strip()]
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY') # Abhi iska istemal nahi ho raha, par rakha hai

class YouTubeServiceManager:
    """API Keys ko manage karne aur error par switch karne ke liye ek class."""
    def __init__(self, api_keys):
        if not api_keys:
            raise ValueError("YouTube API keys configure nahi hain ya khali hain.")
        self.api_keys = api_keys
        self.current_key_index = 0
        self.service = self._build_service()

    def _build_service(self):
        api_key = self.api_keys[self.current_key_index]
        print(f"LOG: YouTube client ko API Key index #{self.current_key_index} (....**{api_key[-4:]}) ke saath banaya ja raha hai.")
        return build('youtube', 'v3', developerKey=api_key)

    def get_current_key(self):
        return self.api_keys[self.current_key_index]

    def switch_key_and_get_service(self):
        """Error aane par agli key par switch karta hai."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        print(f"LOG: API Key mein samasya! Agli Key index #{self.current_key_index} par switch kiya ja raha hai.")
        self.service = self._build_service()
        return self.service

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

# SUDHAR: extract_details mein social media links ka extraction joda gaya hai
def extract_details(description):
    """Description se email, phone aur social media links nikalta hai."""
    emails = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', description)))
    phones = list(set(re.findall(r'(?:\+91)?[ -]?(?:[6-9]\d{2}[ -]?\d{3}[ -]?\d{4}|\d{10})', description)))
    
    instagram = re.search(r'(?:instagram\.com\/|ig:)([a-zA-Z0-9._]+)', description, re.IGNORECASE)
    twitter = re.search(r'(?:twitter\.com\/|x\.com\/|@)([a-zA-Z0-9_]+)', description, re.IGNORECASE)
    linkedin = re.search(r'(?:linkedin\.com\/in\/)([a-zA-Z0-9-]+)', description, re.IGNORECASE)

    # Links ko URL format mein store karein ya sirf username
    instagram_link = f"https://www.instagram.com/{instagram.group(1)}" if instagram else None
    twitter_link = f"https://twitter.com/{twitter.group(1)}" if twitter else None
    linkedin_link = f"https://www.linkedin.com/in/{linkedin.group(1)}" if linkedin else None
    
    return {
        "emails": ", ".join(emails), 
        "phones": ", ".join(phones),
        "instagram_link": instagram_link,
        "twitter_link": twitter_link,
        "linkedin_link": linkedin_link,
    }

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


# --- SUDHAR: update_video_counts function implement kiya gaya hai ---
def update_video_counts(channel_ids):
    """Chune hue channels ke Shorts aur Long Videos ki ginti karta hai."""
    print(f"\n--- Update Video Counts Job Shuru Hua ({len(channel_ids)} channels) ---")
    
    try:
        yt_manager = YouTubeServiceManager(YOUTUBE_API_KEYS)
    except ValueError as e:
        print(f"FATAL ERROR: {e}")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    for channel_id in channel_ids:
        try:
            # 1. Channel details se uploads playlist ID nikalna
            channel_response = yt_manager.service.channels().list(
                part="contentDetails,snippet",
                id=channel_id
            ).execute()

            if not channel_response.get('items'):
                print(f"LOG: Channel ID {channel_id} nahi mila. Skip kar rahe hain.")
                continue

            item = channel_response['items'][0]
            channel_name = item['snippet']['title']
            uploads_playlist_id = item['contentDetails']['relatedPlaylists']['uploads']

            print(f"LOG: '{channel_name}' ({channel_id}) ki video ginti shuru.")
            
            shorts_count = 0
            long_videos_count = 0
            
            # 2. Uploads playlist ke items ko traverse karna
            next_page_token = None
            total_videos_processed = 0
            
            while True:
                try:
                    playlist_response = yt_manager.service.playlistItems().list(
                        playlistId=uploads_playlist_id,
                        part="contentDetails",
                        maxResults=50, # Har baar 50 videos
                        pageToken=next_page_token
                    ).execute()
                except HttpError as e:
                    # Agar playlist items fetch karte waqt error aaye, to key switch ya skip
                    print(f"LOG: Playlist fetch error for {channel_name}: {e.reason}. Skipping channel.")
                    break
                
                video_ids = [item['contentDetails']['videoId'] for item in playlist_response.get('items', [])]
                if not video_ids:
                    break # Agar aur videos na hon
                
                # 3. Videos ki details (duration) fetch karna
                video_response = None
                try:
                    video_response = yt_manager.service.videos().list(
                        part="contentDetails",
                        id=",".join(video_ids)
                    ).execute()
                except HttpError as e:
                    print(f"LOG: Video details fetch error for {channel_name}: {e.reason}. Skipping batch.")
                    # Agar video details fetch karte waqt error aaye, to agle channel par chale jayenge
                    break 
                    
                for video_item in video_response.get('items', []):
                    # Duration format: PT#M#S
                    duration_str = video_item['contentDetails']['duration']
                    
                    # 'PT' ko hata dein
                    duration_str = duration_str[2:] 
                    
                    # Seconds mein convert karne ka ek aasaan tarika (Shorts < 60 seconds hote hain)
                    # Simple check: Agar duration mein 'M' ya 'H' nahi hai aur 'S' ka count 60 se kam hai, toh short.
                    is_short = ('M' not in duration_str and 'H' not in duration_str and 
                                (int(re.search(r'(\d+)S', duration_str).group(1)) if re.search(r'(\d+)S', duration_str) else 0) < 60)
                    
                    if is_short:
                        shorts_count += 1
                    else:
                        long_videos_count += 1

                total_videos_processed += len(video_ids)
                print(f"  Processed {total_videos_processed} videos...")

                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break
                time.sleep(0.5) # API Quota ka dhyan rakhte hue thoda wait

            # 4. Database mein update karna
            cur.execute("""
                UPDATE channels 
                SET short_videos_count = %s, long_videos_count = %s, retrieved_at = CURRENT_TIMESTAMP
                WHERE channel_id = %s;
            """, (shorts_count, long_videos_count, channel_id))
            conn.commit()
            print(f"SUCCESS: '{channel_name}' updated. Shorts: {shorts_count}, Long: {long_videos_count}")

        except Exception as e:
            print(f"ERROR: Channel {channel_id} update karte samay anjaan error: {e}")
            conn.rollback()
            
    cur.close()
    conn.close()
    print("\n--- Update Video Counts Job Poora Hua ---\n")
# --- Update Video Counts function end ---


def find_channels(category, date_after, min_subs, max_subs, max_channels_limit, require_contact):
    print("\n--- Naya Channel Search Job Shuru Hua ---")
    print(f"Parameters: Category='{category}', After='{date_after}', Subs='{min_subs}-{max_subs}', Limit='{max_channels_limit}'")

    try:
        yt_manager = YouTubeServiceManager(YOUTUBE_API_KEYS)
    except ValueError as e:
        print(f"FATAL ERROR: {e}")
        return

    keywords = CATEGORY_KEYWORDS.get(category, "").split(',')
    search_after_date = datetime.strptime(date_after, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%SZ')
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM app_state WHERE key = 'quota_status'")
        conn.commit()
    except Exception:
        conn.rollback()

    new_channels_found = 0
    processed_channel_ids = set()
    cur.execute("SELECT channel_id FROM channels")
    processed_channel_ids.update(row[0] for row in cur.fetchall())
    print(f"LOG: Database mein pehle se {len(processed_channel_ids)} channels hain.")
    
    for keyword in keywords:
        keyword = keyword.strip()
        if not keyword or new_channels_found >= max_channels_limit:
            break
        
        print(f"\nLOG: Keyword '{keyword}' ke liye search shuru.")
        next_page_token = None
        
        while new_channels_found < max_channels_limit:
            search_response = None
            for i in range(len(yt_manager.api_keys) + 1):
                try:
                    youtube = yt_manager.service
                    print(f"LOG: Page fetch karne ki koshish... (Key index: {yt_manager.current_key_index})")
                    search_response = youtube.search().list(
                        q=keyword, part="snippet", type="channel", maxResults=50,
                        publishedAfter=search_after_date, pageToken=next_page_token
                    ).execute()
                    break # Agar safal, to is loop se bahar
                except HttpError as e:
                    print(f"LOG: API Error! Status: {e.resp.status}, Reason: {e.reason}")
                    if e.resp.status in [403, 400]: # Forbidden, Quota, Invalid Key, etc.
                        if i < len(yt_manager.api_keys) - 1:
                            yt_manager.switch_key_and_get_service()
                            continue
                        else:
                            print("FATAL ERROR: Sabhi API keys fail ho gayi hain. Worker ruk raha hai.")
                            error_message = f"All API keys are failing. Please check keys in Google Cloud Console. Last error: {e.reason}"
                            cur.execute("INSERT INTO app_state (key, value) VALUES ('quota_status', %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;", (error_message,))
                            conn.commit()
                            conn.close()
                            return
                    else:
                        print(f"LOG: Anjaan HttpError, is keyword ko skip kar rahe hain: {e}")
                        break # Is anjaan error par is keyword ko chhod do
            
            if not search_response:
                print(f"LOG: Keyword '{keyword}' ke liye data fetch nahi ho saka. Agle keyword par ja rahe hain.")
                break

            channel_items = search_response.get('items', [])
            if not channel_items:
                print(f"LOG: Keyword '{keyword}' ke liye is page par aur channels nahi mile.")
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
                channel_details_response = yt_manager.service.channels().list(part="snippet,statistics", id=",".join(channel_ids)).execute()
            except HttpError as e:
                print(f"LOG: Channel details fetch karte samay error ({e.reason}). Is batch ko skip kar rahe hain.")
                continue

            for item in channel_details_response.get('items', []):
                if new_channels_found >= max_channels_limit: break
                
                channel_id = item['id']
                channel_name = item['snippet'].get('title', 'N/A')
                
                description = item.get('snippet', {}).get('description', '')
                # SUDHAR: Ab yeh social media links bhi niklega
                details = extract_details(description) 
                
                if require_contact and not (details['emails'] or details['phones']):
                    print(f"LOG: Skip - '{channel_name}' ke paas contact info nahi hai.")
                    continue

                stats = item.get('statistics', {})
                if stats.get('hiddenSubscriberCount', False):
                    print(f"LOG: Skip - '{channel_name}' ke subscribers hidden hain.")
                    continue
                
                subscriber_count = int(stats.get('subscriberCount', 0))
                if not (min_subs <= subscriber_count <= max_subs):
                    print(f"LOG: Skip - '{channel_name}' subscriber range ({subscriber_count}) mein nahi hai.")
                    continue
                
                print(f"LOG: Channel process ho raha hai: '{channel_name}'")
                channel_info = {
                    "channel_id": channel_id, "channel_name": channel_name, "subscriber_count": subscriber_count,
                    "creation_date": item['snippet'].get('publishedAt', '')[:10], "emails": details['emails'],
                    "phone_numbers": details['phones'], "description": description, "category": category,
                    # SUDHAR: Naye social media fields jode gaye
                    "instagram_link": details['instagram_link'],
                    "twitter_link": details['twitter_link'],
                    "linkedin_link": details['linkedin_link'],
                }
                cur.execute("""
                    INSERT INTO channels (channel_id, channel_name, subscriber_count, creation_date, emails, phone_numbers, description, category, instagram_link, twitter_link, linkedin_link)
                    VALUES (%(channel_id)s, %(channel_name)s, %(subscriber_count)s, %(creation_date)s, %(emails)s, %(phone_numbers)s, %(description)s, %(category)s, %(instagram_link)s, %(twitter_link)s, %(linkedin_link)s)
                    ON CONFLICT (channel_id) DO NOTHING;
                """, channel_info)

                if cur.rowcount > 0:
                    new_channels_found += 1
                    conn.commit()
                    print(f"SUCCESS: Naya channel save hua: '{channel_name}' ({new_channels_found}/{max_channels_limit})")

            next_page_token = search_response.get('nextPageToken')
            if not next_page_token:
                print(f"LOG: Keyword '{keyword}' ke liye sabhi pages poore hue.")
                break
            time.sleep(1)
            
    cur.close()
    conn.close()
    print("\n--- Channel Search Job Poora Hua ---\n")
