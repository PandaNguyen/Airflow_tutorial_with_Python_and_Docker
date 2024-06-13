from flask import Flask, request, redirect, session, url_for
import requests
import base64
import secrets
import json
from urllib.parse import urlencode

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)  # Use a securely generated secret key

CLIENT_ID = '74411dce5dec4ade9b1ed11f1a5f2dfc'
CLIENT_SECRET = 'f8bb58d851e84258af3d99f1a883c989'
REDIRECT_URI = 'http://localhost:8080/callback'  # Ensure this matches Spotify's registered redirect URI
SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/authorize'
SPOTIFY_TOKEN_URL = 'https://accounts.spotify.com/api/token'
SCOPES = 'user-read-recently-played'

@app.route('/')
def index():
    return "Welcome to the Spotify Auth Example. Go to /home to start the authentication process."

@app.route('/home')
def home():
    auth_query_parameters = {
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'scope': SCOPES,
        'client_id': CLIENT_ID
    }
    auth_url = f"{SPOTIFY_AUTH_URL}?{urlencode(auth_query_parameters)}"
    return redirect(auth_url)

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return "Error: Authorization code not found."

    token_data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(SPOTIFY_TOKEN_URL, data=token_data, headers=headers)
    if response.status_code == 200:
        data = response.json()
        access_token = data['access_token']
        session['access_token'] = access_token

        file_path = "access_token.txt"
        with open(file_path, "w") as file:
            file.write(access_token)
        print(f"Access token saved to {file_path}")
        return "Access token obtained successfully and stored."
    else:
        return f"Failed to obtain an access token. Status code: {response.status_code}, Response: {response.text}"

if __name__ == '__main__':
    app.run(debug=True, port=8080)
