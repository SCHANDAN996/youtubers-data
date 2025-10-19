import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify
from dotenv import load_dotenv
from youtube_logic import init_db, find_channels, get_db_connection
import io
from datetime import datetime, timedelta

load_dotenv()
app = Flask(__name__)
app.secret_key = os.urandom(24)

with app.app_context():
    init_db()

@app.route('/')
def index():
    # Sirf search form dikhayega
    default_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    return render_template('index.html', default_date=default_date)

@app.route('/search', methods=['POST'])
def search():
    try:
        category = request.form.get('category')
        start_date = request.form.get('start_date')
        min_subs = int(request.form.get('min_subs', 0))
        max_subs = int(request.form.get('max_subs', 1000000))
        max_channels = int(request.form.get('max_channels', 100))
        # Naya checkbox value
        require_contact = 'require_contact' in request.form
        
        find_channels(category, start_date, min_subs, max_subs, max_channels, require_contact)
        flash("Search poora hua!", "success")
    except Exception as e:
        flash(f"Ek error aa gaya: {e}", "error")
    # Results page par redirect karega
    return redirect(url_for('results'))

@app.route('/results')
def results():
    # Database se data dikhayega
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT channel_id, channel_name, subscriber_count, category, emails, phone_numbers, 
               instagram_link, twitter_link, linkedin_link, status
        FROM channels ORDER BY retrieved_at DESC;
    """)
    channels = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('results.html', channels=channels, channel_count=len(channels))

@app.route('/update_status', methods=['POST'])
def update_status():
    try:
        data = request.get_json()
        channel_id, new_status = data['channel_id'], data['status']
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE channels SET status = %s WHERE channel_id = %s", (new_status, channel_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
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
