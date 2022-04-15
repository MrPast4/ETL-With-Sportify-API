import spotipy  # spotify sdk
import requests  # to send HTTP requests
from datetime import datetime  # to track the current time
import boto3  # aws sdk
import pandas as pd  # data manipulation

from spotipy.oauth2 import SpotifyClientCredentials

# ================ global variables ================
# ================ authentication ================
CLIENT_ID = 'to_fill_your_spotify_client_id'
CLIENT_SECRET = 'to_fill_your_spotify_client_secret'

AUTH_URL = 'https://accounts.spotify.com/api/token'

# POST Request
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})

# convert the response to JSON
auth_response_data = auth_response.json()

# save the access token
access_token = auth_response_data['access_token']

# verify that the variable is not empty
print("The Token: ", access_token)

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}

# base URL of all Spotify API endpoints
BASE_URL = 'https://api.spotify.com/v1/'


def get_hardstyle_daily():
    # variables
    single_song_dict = {}
    multiple_song_dict = {}
    counter = 0
    song_key = []
    song_value = []

    # hardstyle playlist
    playlist_id = '3aR2n0XpRNlrWose8kx82S'
    # pull all artists albums
    response = requests.get(BASE_URL + 'playlists/' + playlist_id + '/tracks',
                            headers=headers,
                            params={'include_groups': 'album', 'limit': 50})
    # inspect the output optional with a json viewer
    answered_json = response.json()

    """:TODO:
    What i want from a single song:
    - Name
    - Artist(s)
    - Release Date
    - Duration in ms
    - href => optional to extract ID
    - Popularity
    """

    # prepare the list with the meaning to each variable
    song_key.extend(['song_name', 'artist_name', 'release_date', 'duration_in_ms', 'href', 'popularity'])
    # loop over albums and get all tracks
    for song in answered_json['items']:
        song_name = song['track']['album']['name']
        # iterate through the artists and save the result in a list
        artist_name = [i['name'] for i in song['track']['artists']]
        release_date = song['track']['album']['release_date']
        duration = song['track']['duration_ms']
        href = song['track']['href']
        popularity = song['track']['popularity']

        # create the pair with the collected info
        song_value.extend([song_name, artist_name, release_date, duration, href, popularity])

        for i in range(len(song_key)):
            single_song_dict[song_key[i]] = song_value[i]

        # fill the dictionary with the info of one song
        multiple_song_dict[counter] = single_song_dict

        # jettison the list with the specific song info
        song_value = []
        single_song_dict = {}
        print("The song's name:", song_name)
        print("The artist(s):", artist_name)
        print("Anchor Tag:", href)
        print("================================================================")
        counter += 1

    print("The iterations:", counter)
    print("The dict:", multiple_song_dict)

    df = pd.DataFrame(multiple_song_dict)
    # print(df.head().T)
    # swap the x with the y-axis
    new_df = df.T

    # catch the current time
    now = datetime.now()

    # format to: dd-mm-YY
    date = now.strftime("%d-%m-%Y")
    # format to: H:M:S
    time = now.strftime("%H:%M:%S")

    # sort after the newest song. If the date is equal sort after the name
    list_of_newest_songs = new_df.sort_values(by=['release_date', 'song_name'], ascending=[False, True])

    path_to_save_csv = r'/Users/pascal/Desktop/Python/ETL/newest_hardstyle_' + date + '_' + time + '.csv'

    # TODO: add timestamp to the name of the file
    list_of_newest_songs.to_csv(path_to_save_csv, index=False, header=True)

    print(list_of_newest_songs)

    # returns a list with two parts => ['/Users/pascal/Desktop/Python', '/newest_hardstyle_20-03-2022_12:39:57.csv']
    # and we want the second element
    name_of_file = path_to_save_csv.split("/ETL")[1]

    # https://www.youtube.com/watch?v=G68oSgFotZA
    # install aws cli
    # aws configure
    # AWS Access Key ID [****************455M]: to_fill_your_access_key_id
    # AWS Secret Access Key [****************uD4J]: to_fill_your_secret_access_key
    # Default region name [None]: eu-central-1
    # Default output format [None]: keep it empty

    s3 = boto3.client('s3')
    s3.upload_file(path_to_save_csv, 'to_fill_your_buckets_name', name_of_file)


if __name__ == '__main__':
    get_hardstyle_daily()
