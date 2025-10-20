import time
import json
import os
from dotenv import load_dotenv

load_dotenv()
from app.services import youtube_service

def process_jobs():
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
                print(f"\nLOG: Naya job #{job_id} uthaya gaya. Parameters: {params}")

                # Abhi hum sirf 'find_channels' job type ko support kar rahe hain
                youtube_service.find_channels(
                    category=params['category'],
                    date_after=params['start_date'],
                    min_subs=params['min_subs'],
                    max_subs=params['max_subs'],
                    max_channels_limit=params['max_channels'],
                    require_contact=params['require_contact']
                )
                
                cur.execute("UPDATE jobs SET status = 'completed', finished_at = CURRENT_TIMESTAMP WHERE id = %s", (job_id,))
                conn.commit()
                print(f"LOG: Job #{job_id} safaltapoorvak poora hua.")
            else:
                # Jab koi job na ho, to 10 second ruko
                time.sleep(10)
        
        except Exception as e:
            print(f"FATAL WORKER ERROR: Worker mein ek badi error aa gayi: {e}")
            if job and conn:
                job_id = job[0]
                try:
                    cur.execute("UPDATE jobs SET status = 'failed' WHERE id = %s", (job_id,))
                    conn.commit()
                    print(f"LOG: Job #{job_id} ko 'failed' mark kar diya gaya hai.")
                except Exception as db_err:
                    print(f"DATABASE ERROR: Failed job ko mark karne mein error: {db_err}")
            time.sleep(15) # Error aane par thoda zyada der ruko
        
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    process_jobs()
