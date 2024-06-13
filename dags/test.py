import requests

# Load the access token from a file
with open('access_token.txt', 'r') as file:
    access_token = file.read().strip()

# Set the authorization header with the loaded access token
headers = {
    'Authorization': f'Bearer {access_token}'
}

# Send a GET request to fetch recently played tracks
response = requests.get('https://api.spotify.com/v1/me/player/recently-played', headers=headers)

# Check the response and handle accordingly
if response.status_code == 200:
    data = response.json()
    print(data)  # Display the data
else:
    # Print error details if the request failed
    print('Failed to retrieve recently played data:', response.status_code, response.text)
