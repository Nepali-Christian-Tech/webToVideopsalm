import random
import re
import psycopg2
import json
import string

# Step 1: Define database connection parameters
db_config = {
    'host': 'localhost',         # e.g., 'localhost' or an IP address
    'database': 'song_update', # e.g., 'mydb'
    'user': 'postgres',         # e.g., 'postgres'
    'password': '1234'  # Replace with your actual password
}

# Step 2: Define the SQL query
query = "SELECT * FROM songs;"  # Replace with your table name

#tags
tag = {"Chorus":1,"Verse":0}

# verse


songs = []

def song_operation(song: str, title):
    temp_verses = song.split("\n\n\n")
    verses = []
    song = {}
    for t_verse in temp_verses:
        s=t_verse.count("\n")
        verse = {}
        pattern = r'^(Chorus|Verse|Ending)\s*(\d+)?(?::|$)'
        match = re.match(pattern, t_verse)
        if match:
            prefix = match.group(1)  # e.g., "Chorus", "Verse", "Ending"
            number = match.group(2)  # e.g., "1" or None
            number = int(number) if number else 0  # Default to 0 if no number
            if number!=0:
                verse["ID"]= number
            if(prefix == "Chorus"):
                verse["Tag"]= 1

        verse['Text'] = t_verse
        verses.append(verse)
    song["Verses"] = verses
    song["Guid"] = generate_videopsalm_guid()
    song["Text"] = title
    songs.append(song)  
    print(generate_videopsalm_guid())
    # print(song)

def extract_numbers(text, type):
    """
    Extracts all numbers from a string, including:
    - Integers (e.g., 123)
    - Decimals (e.g., 45.67)
    - Negative numbers (e.g., -89)
    
    Returns a list of numeric values (int/float)
    """
    matches = re.findall(r'-?\d+\.?\d*', text)
    return [float(n) if '.' in n else int(n) for n in matches]

def generate_videopsalm_guid(length=22):
    return 'NbgYYzQBz9l7V001fiK4v0'
    # Generate a random alphanumeric string of the given length
    characters = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    return ''.join(random.choice(characters) for _ in range(length))

# Step 3: Connect to the database and fetch data
try:
    # Establish a connection to the database
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)
    rows = cursor.fetchall()

    songBook = {}
    # Step 4: Dynamically generate file names based on a column value
    for index,row in enumerate(rows):
        # Assume the first column (e.g., 'id') will be used as the file name
        file_name = f"{row[0]}-{row[1]}"  # Use the first column value as the file name

        song_operation(str.strip(row[3]), file_name)
        # if index == 2:
        #     break
        # Write data to the dynamically named file
        # with open(file_name, "w", encoding="utf-8") as file:
        #     # Write column headers (optional)
        #     # colnames = [desc[0] for desc in cursor.description]
        #     # file.write("\t".join(colnames) + "\n")  # Tab-separated headers

        #     # Write the current row of data
        #     file.write(row[4])  # Tab-separated values

        print(f"Data successfully saved to {file_name}")
    songBook["Songs"] = songs 
    songBook["ID"] = generate_videopsalm_guid()
    songBook["IsCompressed"] = 0
    songBook["Text"] = "song" # Pretty print with 4-space indentation

    songBookStr = json.dumps(songBook, ensure_ascii=False)

    pattern = rf'"(\w+)":'
    replacement = rf'\1:'

    songBookStr = re.sub(pattern, replacement, songBookStr)

    pattern = rf'(Chorus|Verse|Ending)\s*(\d+)?(?::|$)\\n":'
    replacement = rf''

    songBookStr = re.sub(pattern, replacement, songBookStr)

    pattern = rf'\\n'
    replacement = rf'\n'

    songBookStr = re.sub(pattern, replacement, songBookStr)

    pattern = rf'२\n२'
    replacement = rf'२'

    songBookStr = re.sub(pattern, replacement, songBookStr)



    with open("data.json", "w", encoding="utf-8") as file:
        json.dump(songBookStr, file, ensure_ascii=False)
    # print(json_string)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Step 5: Close the database connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()