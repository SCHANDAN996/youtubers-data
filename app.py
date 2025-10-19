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
        require_contact = 'require_contact' in request.form
        
        if not all([category, start_date]) or min_subs is None or max_subs is None or max_channels is None:
            flash("Sabhi fields zaroori hain.", "error")
            return redirect(url_for('index'))

        print(f"Search shuru: category={category}, after={start_date}, subs={min_subs}-{max_subs}, limit={max_channels}, require_contact={require_contact}")
        find_channels(category, start_date, min_subs, max_subs, max_channels, require_contact)
        flash("Search poora hua! Naye channels (agar mile) dashboard mein jod diye gaye hain.", "success")

    except Exception as e:
        print(f"Search ke dauraan error: {e}")
        flash(f"Ek error aa gaya: {e}", "error")

    return redirect(url_for('results'))

@app.route('/results')
def results():
    conn = get_db_connection()
    cur = conn.cursor()

    # **IMPROVEMENT**: Sorting and Filtering logic
    sort_by = request.args.get('sort_by', 'retrieved_at')
    sort_order = request.args.get('sort_order', 'DESC')
    search_query = request.args.get('query', '').strip()
    filter_category = request.args.get('category_filter', '').strip()

    # Build SQL query dynamically for sorting and filtering
    sql_query = """
        SELECT channel_id, channel_name, subscriber_count, category, emails, phone_numbers, 
               instagram_link, twitter_link, linkedin_link, status,
               short_videos_count, long_videos_count
        FROM channels
    """
    where_clauses = []
    params = {}

    if search_query:
        where_clauses.append("(channel_name ILIKE %(search_query)s OR emails ILIKE %(search_query)s OR description ILIKE %(search_query)s)")
        params['search_query'] = f'%{search_query}%'
    
    if filter_category:
        where_clauses.append("category = %(filter_category)s")
        params['filter_category'] = filter_category
    
    if where_clauses:
        sql_query += " WHERE " + " AND ".join(where_clauses)
    
    # Sanitize sort_by to prevent SQL injection (only allow known columns)
    allowed_sort_cols = {
        'channel_name', 'subscriber_count', 'category', 'creation_date', 
        'retrieved_at', 'short_videos_count', 'long_videos_count'
    }
    if sort_by not in allowed_sort_cols:
        sort_by = 'retrieved_at' # Default if invalid sort_by
    
    # Sanitize sort_order
    if sort_order.upper() not in ['ASC', 'DESC']:
        sort_order = 'DESC' # Default if invalid sort_order

    sql_query += f" ORDER BY {sort_by} {sort_order}"

    cur.execute(sql_query, params)
    channels = cur.fetchall()
    cur.close()
    conn.close()
    
    # Provide all categories for the filter dropdown
    all_categories = list(youtube_logic.CATEGORY_KEYWORDS.keys())

    return render_template(
        'results.html', 
        channels=channels, 
        channel_count=len(channels),
        all_categories=all_categories,
        current_sort_by=sort_by,
        current_sort_order=sort_order,
        current_search_query=search_query,
        current_filter_category=filter_category
    )

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
