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
        job = None
        try:
            conn = youtube_service.get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                UPDATE jobs SET status = 'running', started_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM jobs WHERE status = 'pending' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
                ) RETURNING id, params;
            """)
            job = cur.fetchone()
            conn.commit()

            if job:
                job_id, params = job
                print(f"Job #{job_id} uthaya gaya. Type: {params.get('type')}")

                # SUDHAR: Job ke type ke aadhar par function call karein
                job_type = params.get('type', 'find_channels') # Default purana behavior

                if job_type == 'find_channels':
                    youtube_service.find_channels(
                        category=params['category'],
                        date_after=params['start_date'],
                        min_subs=params['min_subs'],
                        max_subs=params['max_subs'],
                        max_channels_limit=params['max_channels'],
                        require_contact=params['require_contact']
                    )
                elif job_type == 'update_videos':
                    youtube_service.update_video_counts_for_channels(
                        channel_ids=params['channel_ids']
                    )
                
                cur.execute("UPDATE jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP WHERE id = %s", (job_id,))
                conn.commit()
                print(f"Job #{job_id} safaltapoorvak poora hua.")
            else:
                time.sleep(10)
        
        except Exception as e:
            print(f"Worker mein ek error aa gaya: {e}")
            if job and conn:
                job_id = job[0]
                try:
                    cur.execute("UPDATE jobs SET status = 'failed' WHERE id = %s", (job_id,))
                    conn.commit()
                except Exception as db_err:
                    print(f"Failed job ko mark karne mein error: {db_err}")
            time.sleep(15)
        
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    process_jobs()

