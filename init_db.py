import os
import psycopg2
from dotenv import load_dotenv

# .env फ़ाइल से DATABASE_URL लोड करें
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

def initialize_database():
    """
    Database में सभी ज़रूरी टेबल बनाता है।
    """
    # सभी टेबल बनाने के लिए SQL कमांड्स
    commands = (
        """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name TEXT NOT NULL,
            subscriber_count BIGINT,
            creation_date DATE,
            emails TEXT,
            phone_numbers TEXT,
            description TEXT,
            category VARCHAR(100),
            status VARCHAR(50) DEFAULT 'New',
            retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            instagram_link TEXT,
            twitter_link TEXT,
            linkedin_link TEXT,
            short_videos_count INTEGER,
            long_videos_count INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            params JSONB,
            status VARCHAR(50) DEFAULT 'pending',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS app_state (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT
        );
        """
    )
    
    conn = None
    try:
        # डेटाबेस से कनेक्ट करें
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # हर कमांड को चलाएं
        for command in commands:
            cur.execute(command)
        
        # बदलावों को सेव करें
        cur.close()
        conn.commit()
        print("✅ डेटाबेस टेबल सफलतापूर्वक बना दिए गए हैं!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ डेटाबेस में एरर आया: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    initialize_database()
