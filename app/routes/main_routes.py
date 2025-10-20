# BUG FIX: send_file ko import kiya gaya
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from app.services import youtube_service
import pandas as pd
import io
from datetime import datetime, timedelta
import json

main_bp = Blueprint('main', __name__)

# Baki sabhi code waisa hi rahega... (koi aur badlav nahi)
@main_bp.route('/')
def index():
    default_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    return render_template('index.html', default_date=default_date)

@main_bp.route('/search', methods=['POST'])
def search():
    try:
        job_params = {
            'category': request.form.get('category'),
            'start_date': request.form.get('start_date'),
            'min_subs': int(request.form.get('min_subs', 0)),
            'max_subs': int(request.form.get('max_subs', 1000000)),
            'max_channels': int(request.form.get('max_channels', 100)),
            'require_contact': 'require_contact' in request.form
        }
        conn = youtube_service.get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO jobs (params) VALUES (%s)", (json.dumps(job_params),))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for('main.loading'))
    except Exception as e:
        flash(f"Job shuru karne mein error aa gaya: {e}", "error")
        return redirect(url_for('main.index'))

@main_bp.route('/loading')
def loading():
    return render_template('loading.html')

@main_bp.route('/results')
def results():
    conn = youtube_service.get_db_connection()
    cur = conn.cursor()
    sort_by = request.args.get('sort_by', 'retrieved_at')
    sort_order = request.args.get('sort_order', 'DESC')
    search_query = request.args.get('query', '').strip()
    filter_category = request.args.get('category_filter', '').strip()
    
    sql_query = "SELECT channel_id, channel_name, subscriber_count, category, emails, phone_numbers, instagram_link, twitter_link, linkedin_link, status, short_videos_count, long_videos_count FROM channels"
    where_clauses, params = [], {}

    if search_query:
        where_clauses.append("(channel_name ILIKE %(q)s OR emails ILIKE %(q)s)")
        params['q'] = f'%{search_query}%'
    if filter_category:
        where_clauses.append("category = %(cat)s")
        params['cat'] = filter_category
    if where_clauses:
        sql_query += " WHERE " + " AND ".join(where_clauses)
        
    allowed_sort = {'retrieved_at', 'subscriber_count', 'category', 'creation_date', 'short_videos_count', 'long_videos_count'}
    if sort_by not in allowed_sort: sort_by = 'retrieved_at'
    if sort_order.upper() not in ['ASC', 'DESC']: sort_order = 'DESC'

    sql_query += f" ORDER BY {sort_by} {sort_order}"

    cur.execute(sql_query, params)
    channels = cur.fetchall()
    cur.close()
    conn.close()

    all_categories = list(youtube_service.CATEGORY_KEYWORDS.keys())
    return render_template('results.html', channels=channels, channel_count=len(channels), all_categories=all_categories, current_sort_by=sort_by, current_sort_order=sort_order, current_search_query=search_query, current_filter_category=filter_category)

@main_bp.route('/update_status', methods=['POST'])
def update_status():
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
    conn = youtube_service.get_db_connection()
    cur = conn.cursor()
    payload = request.get_json()
    delete_type = payload.get('type')
    if delete_type == 'all':
        cur.execute("TRUNCATE TABLE channels RESTART IDENTITY;")
        flash("Sabhi channels delete kar diye gaye hain.", "success")
    elif delete_type == 'single':
        cur.execute("DELETE FROM channels WHERE channel_id = %s", (payload.get('channel_id'),))
    
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'success': True})

@main_bp.route('/download')
def download():
    conn = youtube_service.get_db_connection()
    # SUDHAR: Query ko behtar banaya gaya
    df = pd.read_sql_query("SELECT channel_name, subscriber_count, category, emails, phone_numbers, instagram_link, twitter_link, linkedin_link, status, short_videos_count, long_videos_count, description, creation_date, retrieved_at FROM channels ORDER BY subscriber_count DESC", conn)
    conn.close()
    
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='youtubers_data.csv')
