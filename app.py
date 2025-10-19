import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify
from dotenv import load_dotenv
from youtube_logic import init_db, find_channels, get_db_connection
import io
from datetime import datetime, timedelta

load_dotenv()
app = Flask(__name__)
app.secret_key = os.urandom(24) # Flash messages ke liye zaroori hai

# App shuru hone par database check/initialize karein
with app.app_context():
    init_db()

@app.route('/')
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()
    # Status column bhi fetch karein
    cur.execute("SELECT channel_id, channel_name, subscriber_count, category, emails, creation_date, ai_summary, ai_tone, ai_audience, status FROM channels ORDER BY retrieved_at DESC;")
    channels = cur.fetchall()
    cur.close()
    conn.close()
    
    # Date picker ke liye default date
    default_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    return render_template('dashboard.html', channels=channels, channel_count=len(channels), default_date=default_date)

@app.route('/search', methods=['POST'])
def search():
    try:
        category = request.form.get('category')
        start_date = request.form.get('start_date')
        min_subs = int(request.form.get('min_subs', 0))
        max_subs = int(request.form.get('max_subs', 1000000))
        max_channels = int(request.form.get('max_channels', 100))
        
        if not all([category, start_date, min_subs, max_subs, max_channels]):
            flash("Sabhi fields zaroori hain.", "error")
            return redirect(url_for('dashboard'))

        print(f"Search shuru: category={category}, after={start_date}, subs={min_subs}-{max_subs}, limit={max_channels}")
        find_channels(category, start_date, min_subs, max_subs, max_channels)
        flash("Search poora hua! Naye channels (agar mile) dashboard mein jod diye gaye hain.", "success")

    except Exception as e:
        print(f"Search ke dauraan error: {e}")
        flash(f"Ek error aa gaya: {e}", "error")

    return redirect(url_for('dashboard'))

# --- NAYA FEATURE: Status update karne ke liye ---
@app.route('/update_status', methods=['POST'])
def update_status():
    try:
        data = request.get_json()
        channel_id = data['channel_id']
        new_status = data['status']
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE channels SET status = %s WHERE channel_id = %s", (new_status, channel_id))
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Status updated successfully.'})
    except Exception as e:
        print(f"Status update karne mein error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/download')
def download():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM channels ORDER BY subscriber_count DESC", conn)
    conn.close()
    
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='youtubers_data.csv')

if __name__ == '__main__':
    app.run(debug=True)
