import time
import json
import os
from dotenv import load_dotenv

# App services import karne se pehle environment variables load karein
load_dotenv()

from app.services import youtube_service

def process_jobs():
    """
    Database se 'pending' jobs ko lagatar check karta hai aur unhe process karta hai.
    """
    print("Worker shuru ho gaya, naye jobs ka intezar...")
    while True:
        conn = None
        job_id = None # Error handling ke liye job_id ko pehle se define karein
        try:
            conn = youtube_service.get_db_connection()
            cur = conn.cursor()
            
            # Ek 'pending' job ko select karo aur use 'running' mark karo
            cur.execute("""
                UPDATE jobs
                SET status = 'running', started_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE status = 'pending'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, params;
            """)
            
            job = cur.fetchone()
            conn.commit()

            if job:
                job_id, params = job
                print(f"Job #{job_id} uthaya gaya. Params: {params}")
                
                # Asli search function ko call karo
                youtube_service.find_channels(
                    category=params['category'],
                    date_after=params['start_date'],
                    min_subs=params['min_subs'],
                    max_subs=params['max_subs'],
                    max_channels_limit=params['max_channels'],
                    require_contact=params['require_contact']
                )
                
                # Job ko 'completed' mark karo
                cur.execute("UPDATE jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP WHERE id = %s", (job_id,))
                conn.commit()
                print(f"Job #{job_id} safaltapoorvak poora hua.")
            else:
                # Agar koi job nahi hai, to 10 second ruko
                time.sleep(10)
        
        except Exception as e:
            print(f"Worker mein ek error aa gaya: {e}")
            if job_id and conn:
                try:
                    cur.execute("UPDATE jobs SET status = 'failed' WHERE id = %s", (job_id,))
                    conn.commit()
                except Exception as db_err:
                    print(f"Failed job ko mark karne mein error: {db_err}")
            time.sleep(15) # Error aane par thoda zyada der ruko
        
        finally:
            if conn:
                cur.close()
                conn.close()

if __name__ == '__main__':
    process_jobs()
