# app.py
import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file
from dotenv import load_dotenv
from youtube_logic import init_db, find_channels, get_db_connection
import io

load_dotenv()
app = Flask(__name__)

with app.app_context():
    init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    keywords = request.form.get('keywords')
    min_subs = int(request.form.get('min_subs', 0))
    max_subs = int(request.form.get('max_subs', 10000))
    months_ago = int(request.form.get('months_ago', 6))
    
    find_channels(keywords, min_subs, max_subs, months_ago)
    
    return redirect(url_for('results'))

@app.route('/results')
def results():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT channel_name, subscriber_count, emails, phone_numbers, creation_date FROM channels ORDER BY subscriber_count DESC;")
    channels = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('results.html', channels=channels, channel_count=len(channels))

@app.route('/download')
def download():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM channels", conn)
    conn.close()
    
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='youtubers_data.csv')

if __name__ == '__main__':
    app.run(debug=True)
