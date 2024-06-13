import csv
import json
import os
import pickle
import requests as r
from base64 import b64encode
from dotenv import load_dotenv

def main():
    # Obtain the Client ID and Client Secret from the .env file.
    load_dotenv()
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    # Obtain the access token necessary to use the API.
    access_token = get_auth_key(client_id, client_secret)

    # Access the file containing the data from your Spotify extended history.
    csv_file = open('extended.csv', encoding='utf-8')
    csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # Skip over headers in CSV file.
    next(csv_reader, None)

    track_ids = [] # List of unique track_ids
    id_list = [] # List of track_ids to be sent to API
    track_dict = {} # Dictionary of all track data

    # Assemble list of unique track IDs.
    for row in csv_reader:
        if row[4] not in track_ids:
            track_ids.append(row[4])

    # Track where we are in the list to ensure the last batch is sent to the API regardless of size
    end_of_list = len(track_ids)
    row_count = 0

    for row in track_ids:
        row_count += 1
        id_list.append(row)
        if len(id_list) == 50 or row_count == end_of_list: # Spotify API allows track data requests in batches of 50
            track_data = get_track_data(access_token, id_list)
            for track in track_data['tracks']:
                artist_name = track['artists'][0]['name']
                artist_id = track['artists'][0]['id']
                album_name = escape_commas(track['album']['name']) # Quote fields containing commas so they'll be recognized as a single field.
                album_id = track['album']['id']
                track_name = escape_commas(track['name'])
                track_id = track['id']
                track_date = track['album']['release_date']
                date_precision = track['album']['release_date_precision']
                track_length = track['duration_ms']
                track_popularity = track['popularity']
                track_explicit = track['explicit']

                track_dict[track_id] = [artist_name, artist_id, album_name, album_id, track_name, track_id, track_date, date_precision, track_length, track_popularity, track_explicit]
            id_list.clear() # Clear the list so the next 50 ids can be loaded

    # Reload the data file so we can access the timestamp and track id of each track
    csv_file = open('extended.csv', encoding='utf-8')
    csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    next(csv_reader, None)

    master_list = []

    for row in csv_reader:
        track = track_dict.get(row[4], 'Not Found')
        if track == 'Not Found':
            continue # If track isn't found, skip to next track.
        timestamp = row[0]
        artist_name = track[0]
        artist_id = track[1]
        album_name = track[2]
        album_id = track[3]
        track_name = track[4]
        track_id = track[5]
        track_date = track[6]
        date_precision = track[7]
        track_length = track[8]
        track_popularity = track[9]
        track_explicit = track[10]

        master_list.append([timestamp, artist_name, artist_id, album_name, album_id, track_name, track_id, track_date, date_precision, track_length, track_popularity,track_explicit])

    # Write headers to the csv file
    headers = 'timestamp,artist_name,artist_id,album_name,album_id,track_name,track_id,track_date,date_precision,track_length,track_popularity,track_explicit\n'
    append_to_file(headers)

    for track in master_list:
        entry = track[0] + ',' + track[1] + ',' + track[2] + ',' + track[3] + ',' + track[4] + ',' + track[5] + ',' + track[6] + ',' + track[7] + ',' + track[8] + ',' + str(track[9]) + ',' + str(track[10]) + ',' + str(track[11]) + "\n"
        append_to_file(entry)

    print('Process complete')
    exit(0)


def get_track_data(access_token, track_list):
    # Convert track list into a comma-separated string
    tracks_list = ""
    for track in track_list:
        tracks_list = tracks_list + track + ","

    # Delete trailing comma from string
    tracks_list = tracks_list[:-1]

    # URL and headers for API request
    url = f"https://api.spotify.com/v1/tracks?ids={tracks_list}"
    headers = {
        "Accept"        : "application/json",
        "Content-Type"  : "application/json",
    }
    headers['Authorization'] = f"Bearer {access_token}"

    # Make request
    myreq = r.get(url, headers=headers)
    content = myreq.content

    # Check if request was successful
    status_code = myreq.status_code
    reason = myreq.headers
    if status_code != 200:
        print(url)
        print("Error: status code:", status_code, " - ", reason)
        exit(-1)

    json_data = json.loads(content)
    json_str = json.dumps(json_data, indent=4)
    return json_data


def get_auth_key(client_id, client_secret):
    headers = {}
    client_str = f"{client_id}:{client_secret}"
    client_str_bytes = client_str.encode('ascii')
    client_str = b64encode( client_str_bytes )
    client_str = client_str.decode('ascii')
    auth_header = f"Basic {client_str}"
    headers['Authorization'] = auth_header
    data = {
        "grant_type" : "client_credentials"
    }
    url = "https://accounts.spotify.com/api/token"
    myreq = r.post(url, headers=headers, data=data)
    status_code = myreq.status_code
    content = myreq.content.decode('ascii')
    json_data = json.loads(content)
    access_token = json_data['access_token']
    return access_token


def escape_commas(str):
    if ',' in str:
        str = "\"" + str + "\""
    return str


def append_to_file(str):
    file = open('extended-output.csv', 'ab')  # Open a file in append mode
    file.write(str.encode('utf-8'))  # Write the track data
    file.close()  # Close the file

main()