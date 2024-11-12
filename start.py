import requests
import json
import re
from bs4 import BeautifulSoup
import os
import unicodedata
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# URL to fetch
url = 'https://www.nepalichristiansongs.net/songs/script/list.js'
baseurl = 'https://www.nepalichristiansongs.net/songs/lyrics/nepali/'

# Headers to send with the request
headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.nepalichristiansongs.net/',
}

# Add this at the top level of the script, outside any function
song_counter = 1

# Database configuration
DB_CONFIG = {
    'dbname': 'song',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS songs (
                song_id VARCHAR(50) PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT,
                lyrics TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        cur.close()
        conn.close()

def save_to_db(song_data):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO songs (song_id, title, url, lyrics)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (song_id) 
            DO UPDATE SET 
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                lyrics = EXCLUDED.lyrics
        ''', (
            song_data['song_id'],
            song_data['title'],
            song_data['url'],
            song_data['lyrics']
        ))
        conn.commit()
        print(f"Saved to database: {song_data['song_id']} - {song_data['title']}")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        cur.close()
        conn.close()

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def extract_verse(script_tag):
    if script_tag:
        script_content = script_tag.string

        # Find the JavaScript array using regex
        match = re.search(r'var\s+anchorList\s*=\s*new\s+Array\(([^)]+)\);', script_content)
        if match:
            js_array_str = match.group(1)
           # Convert the JavaScript array to a dictionary
            anchor_dict = {}
            for item in re.findall(r'"([^"]*)"', js_array_str):
                key, value = item.split('~')
                anchor_dict[int(key)] = value
            return anchor_dict
        else:
            print("anchorList not found.")
            return []
    else:
        print("No <script> tag found.")
        return []

def get_song(song):
    global song_counter
    url = baseurl + song + '.html'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        head_tag = soup.find('center')

        # Initialize variables
        title = ''
        song_id = ''

        # Extract song title and ID
        title_element = song
        if title_element:
            full_title = title_element.strip()
            print(f"Debug - Full title: {full_title}")  # Debug print
            
            # Try to find ID in the title (now including letters after numbers)
            match = re.search(r'(.*?)\s*-\s*([bcBC]\d+[a-zA-Z]*)\s*-?\s*(?:mp3)?', full_title)
            if match:
                title = match.group(1).strip()
                song_id = match.group(2).lower()
                print(f"Debug - Found ID: {song_id}, Title: {title}")  # Debug print
            else:
                # No ID found, use incremental counter and full title
                title = full_title.split('-mp3')[0].strip()  # Remove -mp3 if present
                song_id = f'o{song_counter}'
                song_counter += 1
                print(f"Debug - Using counter: {song_id}, Title: {title}")  # Debug print
        
        # Fallback if title is still empty
        if not title or not song_id:
            title = song.split('-mp3')[0].strip()  # Use the original song parameter
            song_id = song.split('-mp3')[1].strip()
            song_counter += 1
            print(f"Debug - Fallback: {song_id}, Title: {title}")  # Debug print

        content = []
        for tag in head_tag.find_all(['div', 'span']):
            tag_text = tag.get_text(strip=True) if tag.name != 'hr' else '<hr>'
            tag_text = unicodedata.normalize('NFKC', tag_text)
            
            if tag_text == '' and tag.name == 'div': 
                if not content or content[-1] != '\n':
                    content.append('\n')
            elif tag_text != '&nbsp;' and tag_text != '':
                content.append(tag_text)

        formatted_content = {
            'song_id': song_id,
            'title': title,
            'url': url,
            'lyrics': '\n'.join(content)
        }
        
        # Save to database
        save_to_db(formatted_content)
        
        return formatted_content
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")
        return None

# Example query functions
def get_all_songs():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT song_id, title FROM songs ORDER BY created_at")
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def get_song_by_id(song_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM songs WHERE song_id = %s", (song_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()

def search_songs(search_term):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT * FROM songs 
            WHERE title ILIKE %s OR lyrics ILIKE %s
        """, (f'%{search_term}%', f'%{search_term}%'))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# Main script
try:
    # Initialize the database
    init_db()
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        list_js_content = response.text.replace("var songList = ","")
        python_list = json.loads(list_js_content)
        
        successful_songs = 0
        for sng in python_list:
            song_data = get_song(sng)
            if song_data:
                successful_songs += 1
            
        print(f"Successfully saved {successful_songs} songs to database")
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")

except Exception as e:
    print(f"An error occurred: {e}")

