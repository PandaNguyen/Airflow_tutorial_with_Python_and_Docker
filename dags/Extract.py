import pandas as pd
import requests
from datetime import datetime, timedelta

def load_token():
    # Äá»c token tá»« file
    with open('access_token.txt', 'r') as file:
        return file.read().strip()

def return_dataframe():
    # Gá»i hĂ m Ä‘á»ƒ láº¥y token
    TOKEN = load_token()
    
    # Thiáº¿t láº­p header vá»›i token
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    
    # XĂ¡c Ä‘á»‹nh thá»i Ä‘iá»ƒm hĂ´m qua
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 10
    
    # URL request Ä‘áº¿n Spotify API
    url = f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}"
    
    # Thá»±c hiá»‡n request
    response = requests.get(url, headers=headers)
    
    # Kiá»ƒm tra vĂ  xá»­ lĂ½ pháº£n há»“i
    if response.status_code != 200:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame()  # Tráº£ vá» DataFrame trá»‘ng náº¿u cĂ³ lá»—i
    
    data = response.json()
    
    if "items" not in data:
        print(f"Error: 'items' not found in response data.")
        return pd.DataFrame()
    
    # Xá»­ lĂ½ dá»¯ liá»‡u nháº­n Ä‘Æ°á»£c
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    # Táº¡o DataFrame tá»« dá»¯ liá»‡u
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }
    
    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    return song_df

if __name__ == "__main__":
    df = return_dataframe()
    if not df.empty:
        print("Dataframe loaded successfully.")
        print(df.head())
        #df.to_csv('data.csv', sep='\t', index=False)
    else:
        print("No data available or an error occurred.")
