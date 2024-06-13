import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
print('started')
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Function to read token from file
def read_token_from_file(file_path):
	with open(os.path.join(CUR_DIR,file_path), 'r') as file:
		return file.read().strip()

# Reading token from file
TOKEN = read_token_from_file('access_token.txt')

# Function to return a dataframe with recent Spotify plays
def return_dataframe():
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json",
		"Authorization": f"Bearer {TOKEN}"
	}
    
	today = datetime.now()
	yesterday = today - timedelta(days=1)
	yesterday_unix_timestamp = int(yesterday.timestamp()) * 10
	
	url = f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}"
    
   
	response = requests.get(url, headers=headers)


	try:
		data = response.json()
	except json.decoder.JSONDecodeError:
		print("Error decoding JSON from response")
		print("HTTP Status Code:", response.status_code)
		print("Response Body:", response.text)
		return None  


	song_names, artist_names, played_at_list, timestamps = [], [], [], []
	for song in data["items"]:
		song_names.append(song["track"]["name"])
		artist_names.append(song["track"]["album"]["artists"][0]["name"])
		played_at_list.append(song["played_at"])
		timestamps.append(song["played_at"][0:10])

	song_dict = {
		"song_name": song_names,
		"artist_name": artist_names,
		"played_at": played_at_list,
		"timestamp": timestamps
	}
	return pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])


def data_quality(load_df):
	if load_df.empty:
		print('No Songs Extracted')
		return False
	if not pd.Series(load_df['played_at']).is_unique:
		raise Exception("Primary Key Exception, Data Might Contain Duplicates")
	if load_df.isnull().values.any():
		raise Exception("Null values found")

def transform_df(load_df):
	transformed_df = load_df.groupby(['timestamp', 'artist_name'], as_index=False).count()
	transformed_df.rename(columns={'played_at': 'count'}, inplace=True)
	transformed_df["ID"] = transformed_df['timestamp'].astype(str) + "-" + transformed_df["artist_name"]
	return transformed_df[['ID', 'timestamp', 'artist_name', 'count']]

def spotify_etl():
	
	load_df = return_dataframe()
	if load_df is not None:
		data_quality(load_df)
		print(load_df)
		return load_df
	else:
		print("Failed to retrieve or decode data. Check logs for more details.")




#spotify_etl()

