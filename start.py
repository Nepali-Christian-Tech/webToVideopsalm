import requests
import json
import re
from bs4 import BeautifulSoup

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

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def extractVerse(script_tag):
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


def getSong(song):
    url = baseurl + song + '.html'
    # Send a GET request to the URL with headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Load the content into a variable
        list_js_content = remove_html_tags(response.text)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract the JavaScript code from the <script> tag
        script_tag = soup.find('script')
        head_tag = soup.find('center')

        # Initialize a list to hold the extracted content
        content = []

        # Iterate through the tags of interest, including <hr>
        for tag in head_tag.find_all(['div', 'span']):
            # Extract the text and strip any leading/trailing whitespace
            tag_text = tag.get_text(strip=True) if tag.name != 'hr' else '<hr>'
            
            # Handle the case where multiple &nbsp; should be replaced by a single new line
            if tag_text == '' and tag.name == 'div': 
                if not content or content[-1] != '\n':
                    content.append('\n')
            elif tag_text != '&nbsp;' and tag_text != '':
                content.append(tag_text)

        print(content)
        # extractVerse(script_tag)

        # python_list = json.loads(list_js_content)
        # print(python_list)
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")

try:
    # Send a GET request to the URL with headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Load the content into a variable
        list_js_content = response.text.replace("var songList = ","")
        python_list = json.loads(list_js_content)
        for sng in python_list:
            getSong(sng)
        print(python_list)
    else:
        print(f"Failed to retrieve the content. Status code: {response.status_code}")

except requests.RequestException as e:
    # Print any error that occurs
    print(f"An error occurred: {e}")
except json.JSONDecodeError as e:
    print(f"Failed to parse JSON: {e}")

