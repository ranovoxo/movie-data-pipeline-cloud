import os
import requests

# Option 1: Set your API key directly here for testing (recommended only for local/dev use)
TMDB_API_KEY = os.getenv("TMDB_API_KEY") or "your_actual_api_key_here"

# Option 2 (preferred): Set the environment variable before running
# export TMDB_API_KEY=your_actual_api_key_here

def test_tmdb_api():
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "page": 1
    }

    print("🔍 Testing TMDB API connection...")
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("✅ Success! Number of movies returned:", len(data.get("results", [])))
    else:
        print("❌ Failed to fetch data.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)

if __name__ == "__main__":
    test_tmdb_api()
