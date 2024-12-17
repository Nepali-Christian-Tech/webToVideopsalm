import requests
import json
import re
from bs4 import BeautifulSoup
import os
import unicodedata
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import concurrent.futures
import threading
from queue import Queue
import psycopg2.pool

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

# Thread-safe connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,  # Adjust based on your needs
    **DB_CONFIG
)

# Thread-local storage for database connections
thread_local = threading.local()

def get_db_connection():
    try:
        return db_pool.getconn()
    except psycopg2.pool.PoolError:
        print("Waiting for available database connection...")
        return db_pool.getconn()

def return_db_connection(conn):
    db_pool.putconn(conn)

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
        return_db_connection(conn)

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
        return_db_connection(conn)

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

def get_song(song, progress_tracker=None):
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
        
        if progress_tracker:
            progress_tracker.increment()
        
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
        return_db_connection(conn)

def get_song_by_id(song_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM songs WHERE song_id = %s", (song_id,))
        return cur.fetchone()
    finally:
        cur.close()
        return_db_connection(conn)

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
        return_db_connection(conn)

def process_song(song):
    try:
        song_data = get_song(song)
        if song_data:
            save_to_db(song_data)
            return True
        return False
    except Exception as e:
        print(f"Error processing song {song}: {e}")
        return False

def batch_save_to_db(songs_batch):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        args = [(
            song['song_id'],
            song['title'],
            song['url'],
            song['lyrics']
        ) for song in songs_batch]
        
        cur.executemany('''
            INSERT INTO songs (song_id, title, url, lyrics)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (song_id) 
            DO UPDATE SET 
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                lyrics = EXCLUDED.lyrics
        ''', args)
        
        conn.commit()
        print(f"Saved batch of {len(songs_batch)} songs")
    except psycopg2.Error as e:
        print(f"Database error in batch save: {e}")
    finally:
        cur.close()
        return_db_connection(conn)

def process_batch(songs_batch):
    results = []
    for song in songs_batch:
        try:
            song_data = get_song(song)
            if song_data:
                results.append(song_data)
        except Exception as e:
            print(f"Error processing song {song}: {e}")
    
    if results:
        batch_save_to_db(results)
    return len(results)

def main_with_batching():
    try:
        init_db()
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            list_js_content = response.text.replace("var songList = ","")
            python_list = json.loads(list_js_content)
            
            # Create batches of songs
            batch_size = 10
            batches = [python_list[i:i + batch_size] for i in range(0, len(python_list), batch_size)]
            
            successful_songs = 0
            
            # Process batches with ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
                
                for future in concurrent.futures.as_completed(future_to_batch):
                    successful_songs += future.result()
            
            print(f"Processing complete. Successfully saved: {successful_songs} songs")
        else:
            print(f"Failed to retrieve the content. Status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_pool.closeall()

# Progress tracking
class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.lock = threading.Lock()
        
    def increment(self):
        with self.lock:
            self.current += 1
            self.print_progress()
    
    def print_progress(self):
        percentage = (self.current / self.total) * 100
        print(f"Progress: {self.current}/{self.total} ({percentage:.1f}%)")

if __name__ == "__main__":
    main_with_batching()

