from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from app.services import youtube_service
import pandas as pd
import io
from datetime import datetime, timedelta
import json
import threading # <-- थ्रेडिंग मॉड्यूल इम्पोर्ट करें

main_bp = Blueprint('main', __name__)

def get_quota_status_message():
    # ... (यह फंक्शन वैसा ही रहेगा) ...
    conn = None
    try:
        conn = youtube_service.get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_state WHERE key = 'quota_status'")
        result = cur.fetchone()
        return result[0] if result else None
    except Exception: return None
    finally:
        if conn: conn.close()

@main_bp.route('/')
def index():
    # ... (यह फंक्शन वैसा ही रहेगा) ...
    default_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    quota_message = get_quota_status_message()
    return render_template('index.html', default_date=default_date, quota_message=quota_message)

@main_bp.route('/search', methods=['POST'])
def search():
    """
    सर्च पैरामीटर लेता है और find_channels फंक्शन को एक नए बैकग्राउंड थ्रेड में चलाता है।
    """
    try:
        category = request.form.get('category')
        start_date = request.form.get('start_date')
        min_subs = int(request.form.get('min_subs', 0))
        max_subs = int(request.form.get('max_subs', 1000000))
        max_channels = int(request.form.get('max_channels', 100))
        require_contact = 'require_contact' in request.form
        
        # एक नया थ्रेड बनाएं जो youtube_service.find_channels को चलाएगा
        search_thread = threading.Thread(
            target=youtube_service.find_channels,
            args=(category, start_date, min_subs, max_subs, max_channels, require_contact)
        )
        search_thread.start() # थ्रेड को बैकग्राउंड में शुरू करें

        flash("Channel search has been started in the background. Results will appear here shortly.", "success")
        return redirect(url_for('main.loading')) # लोडिंग पेज पर भेजें
        
    except Exception as e:
        flash(f"An error occurred while starting the search: {e}", "error")
        return redirect(url_for('main.index'))

@main_bp.route('/update-video-counts', methods=['POST'])
def update_video_counts():
    """
    यह रूट अभी तक पूरी तरह से लागू नहीं है क्योंकि youtube_service में इसका फंक्शन नहीं है।
    इसे भी थ्रेडिंग का उपयोग करके बनाया जा सकता है।
    """
    flash("Update video counts feature is not fully implemented in this architecture yet.", "info")
    return jsonify({'success': False, 'message': 'Feature not implemented'})

# ... बाकी के सभी रूट्स (loading, results, download, delete, etc.) वैसे ही रहेंगे ...

@main_bp.route('/loading')
def loading():
    quota_message = get_quota_status_message()
    return render_template('loading.html', quota_message=quota_message)

@main_bp.route('/results')
def results():
    quota_message = get_quota_status_message()
    conn = youtube_service.get_db_connection()
    cur = conn.cursor()
    # ... (बाकी का कोड वैसा ही रहेगा) ...
    cur.execute("SELECT channel_id, channel_name, subscriber_count, category, emails, phone_numbers, instagram_link, twitter_link, linkedin_link, status, short_videos_count, long_videos_count FROM channels ORDER BY retrieved_at DESC")
    channels = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('results.html', channels=channels, channel_count=len(channels), quota_message=quota_message)

@main_bp.route('/update_status', methods=['POST'])
def update_status():
    # ... (यह फंक्शन वैसा ही रहेगा) ...
    try:
        data = request.get_json()
        conn = youtube_service.get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE channels SET status = %s WHERE channel_id = %s", (data['status'], data['channel_id']))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@main_bp.route('/delete', methods=['POST'])
def delete():
    # ... (यह फंक्शन वैसा ही रहेगा) ...
    conn = youtube_service.get_db_connection()
    cur = conn.cursor()
    payload = request.get_json()
    delete_type = payload.get('type')
    if delete_type == 'all':
        cur.execute("TRUNCATE TABLE channels RESTART IDENTITY;")
        flash("All channels have been deleted.", "success")
    elif delete_type == 'single':
        cur.execute("DELETE FROM channels WHERE channel_id = %s", (payload.get('channel_id'),))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'success': True})

@main_bp.route('/download')
def download():
    # ... (यह फंक्शन वैसा ही रहेगा) ...
    conn = youtube_service.get_db_connection()
    df = pd.read_sql_query("SELECT channel_name, subscriber_count, category, emails, phone_numbers, instagram_link, twitter_link, linkedin_link, status, short_videos_count, long_videos_count, description, creation_date, retrieved_at FROM channels ORDER BY subscriber_count DESC", conn)
    conn.close()
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='youtubers_data.csv')

