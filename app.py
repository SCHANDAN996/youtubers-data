import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash
from dotenv import load_dotenv
from youtube_logic import init_db, find_channels, get_db_connection
import io
from datetime import datetime, timedelta

load_dotenv()
app = Flask(__name__)
app.secret_key = os.urandom(24) # Needed for flashing messages

with app.app_context():
    init_db()

@app.route('/')
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch all columns for the dashboard
    cur.execute("SELECT channel_name, subscriber_count, category, emails, creation_date, ai_summary, ai_tone, ai_audience FROM channels ORDER BY retrieved_at DESC;")
    channels = cur.fetchall()
    cur.close()
    conn.close()
    
    # Get today's date for the date picker default
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
        
        # --- Simple Validation ---
        if not all([category, start_date, min_subs, max_subs, max_channels]):
            flash("All fields are required.", "error")
            return redirect(url_for('dashboard'))

        print(f"Starting search for category: {category}, after: {start_date}, subs: {min_subs}-{max_subs}, limit: {max_channels}")
        find_channels(category, start_date, min_subs, max_subs, max_channels)
        flash("Search completed! New channels (if any) have been added to the dashboard.", "success")

    except Exception as e:
        print(f"An error occurred during search submission: {e}")
        flash(f"An error occurred: {e}", "error")

    return redirect(url_for('dashboard'))

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

