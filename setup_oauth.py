import os
import psycopg2
import json
import sys

def setup_oauth():
    # Retrieve environment variables
    db_url = os.environ.get("GOAT_DB_URL")
    youtube_client_id = os.environ.get("YOUTUBE_CLIENT_ID", "").strip() or None
    youtube_client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET", "").strip() or None
    gsheets_client_id = os.environ.get("GSHEETS_CLIENT_ID", "").strip() or None
    gsheets_client_secret = os.environ.get("GSHEETS_CLIENT_SECRET", "").strip() or None
    spotify_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip() or None
    spotify_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip() or None
    instagram_id = os.environ.get("INSTAGRAM_CLIENT_ID", "").strip() or None
    instagram_secret = os.environ.get("INSTAGRAM_CLIENT_SECRET", "").strip() or None

    # Validate environment variables
    missing = []
    if not db_url: missing.append("GOAT_DB_URL")
    if not youtube_client_id: missing.append("YOUTUBE_CLIENT_ID")
    if not youtube_client_secret: missing.append("YOUTUBE_CLIENT_SECRET")
    if not gsheets_client_id: missing.append("GSHEETS_CLIENT_ID")
    if not gsheets_client_secret: missing.append("GSHEETS_CLIENT_SECRET")

    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # 1. Update YouTube Provider
        youtube_metadata = {
            "oauth": {
                "client_id": youtube_client_id,
                "client_secret": youtube_client_secret,
                "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
                "token_url": "https://oauth2.googleapis.com/token",
                "scopes": "https://www.googleapis.com/auth/youtube.readonly",
                "redirect_uri": "https://sequels.diy/auth/plugin/callback"
            }
        }
        
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'youtube',
                auth_types = %s,
                metadata_schema = %s
            WHERE name = 'YouTube';
            """,
            (json.dumps(["oauth2"]), json.dumps(youtube_metadata))
        )
        print(f"YouTube update executed. Rows affected: {cur.rowcount}")

        # 2. Update Spotify Provider
        spotify_metadata = {
            "oauth": {
                "client_id": spotify_id,
                "client_secret": spotify_secret,
                "authorize_url": "https://accounts.spotify.com/authorize",
                "token_url": "https://accounts.spotify.com/api/token",
                "scopes": "playlist-modify-public playlist-modify-private user-library-modify playlist-read-private user-read-private",
                "redirect_uri": "https://sequels.diy/auth/plugin/callback"
            }
        }
        
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'spotify',
                auth_types = %s,
                metadata_schema = %s
            WHERE name = 'Spotify';
            """,
            (json.dumps(["oauth2"]), json.dumps(spotify_metadata))
        )
        print(f"Spotify update executed. Rows affected: {cur.rowcount}")

        # 3. Update Instagram Provider
        instagram_metadata = {
            "oauth": {
                "client_id": instagram_id,
                "client_secret": instagram_secret,
                "authorize_url": "https://www.instagram.com/oauth/authorize",
                "token_url": "https://api.instagram.com/oauth/access_token",
                "scopes": "instagram_business_basic,instagram_business_manage_messages,instagram_business_manage_comments,instagram_business_content_publish,instagram_business_manage_insights",
                "redirect_uri": "https://sequels.diy/auth/plugin/callback"
            }
        }
        
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'instagram',
                auth_types = %s,
                metadata_schema = %s
            WHERE name = 'Instagram';
            """,
            (json.dumps(["oauth2"]), json.dumps(instagram_metadata))
        )
        print(f"Instagram update executed. Rows affected: {cur.rowcount}")

        # 4. Update X.com (Twitter) Provider
        x_id = os.environ.get("X_CLIENT_ID", os.environ.get("TWITTER_CLIENT_ID", "")).strip() or None
        x_secret = os.environ.get("X_CLIENT_SECRET", os.environ.get("TWITTER_CLIENT_SECRET", "")).strip() or None
        x_metadata = {
            "oauth": {
                "client_id": x_id,
                "client_secret": x_secret,
                "authorize_url": "https://twitter.com/i/oauth2/authorize",
                "token_url": "https://api.x.com/2/oauth2/token",
                "scopes": "tweet.write tweet.read users.read offline.access",
                "redirect_uri": "https://sequels.diy/auth/plugin/callback"
            }
        }
        
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'X',
                auth_types = %s,
                metadata_schema = %s
            WHERE name = 'X';
            """,
            (json.dumps(["oauth2"]), json.dumps(x_metadata))
        )
        print(f"X.com update executed. Rows affected: {cur.rowcount}")

        # 5. Update Google Sheets Provider
        googlesheets_metadata = {
            "oauth": {
                "client_id": gsheets_client_id,
                "client_secret": gsheets_client_secret,
                "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
                "token_url": "https://oauth2.googleapis.com/token",
                "scopes": "https://www.googleapis.com/auth/spreadsheets",
                "redirect_uri": "https://sequels.diy/auth/plugin/callback"
            }
        }
        
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'sheets',
                auth_types = %s,
                metadata_schema = %s
            WHERE name = 'Google Sheets';
            """,
            (json.dumps(["oauth2"]), json.dumps(googlesheets_metadata))
        )
        print(f"Google Sheets update executed. Rows affected: {cur.rowcount}")



        ## update telegram provider
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'telegram'
            WHERE name = 'Telegram';
            """
        )
        print(f"Telegram plugin update executed. Rows affected: {cur.rowcount}")

        ## update github provider
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'github'
            WHERE name = 'GitHub';
            """
        )
        print(f"Github plugin update executed. Rows affected: {cur.rowcount}")

        ## update rss provider
        cur.execute(
            """
            UPDATE plugin_providers 
            SET icon = 'rss'
            WHERE name = 'RSS Feed';
            """
        )
        print(f"RSS plugin update executed. Rows affected: {cur.rowcount}")


        # Commit and close
        conn.commit()
        cur.close()
        conn.close()
        print("Successfully updated plugin providers.")

    except Exception as e:
        print(f"Database error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_oauth()
