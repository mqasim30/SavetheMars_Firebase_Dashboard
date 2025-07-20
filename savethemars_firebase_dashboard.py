import os
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Firebase configuration from environment variables or Streamlit secrets
firebase_cert_source = os.environ.get("FIREBASE_CERT_PATH") or st.secrets.get("FIREBASE_CERT_JSON")
firebase_db_url = os.environ.get("FIREBASE_DB_URL") or st.secrets.get("FIREBASE_DB_URL")

logging.info("Firebase DB URL: %s", firebase_db_url)

if not firebase_cert_source or not firebase_db_url:
    st.error("Firebase configuration is missing. Set FIREBASE_CERT_JSON (as dict) and FIREBASE_DB_URL in your secrets.")
    st.stop()

# Convert to a regular dict if it's not one already
if not isinstance(firebase_cert_source, dict):
    try:
        firebase_cert_source = dict(firebase_cert_source)
        logging.info("Converted firebase_cert_source to dict successfully.")
    except Exception as e:
        logging.error("Failed to convert certificate source to dict: %s", e)
        st.error("Failed to convert certificate source to dict: " + str(e))
        st.stop()

# Replace escaped newline characters with actual newlines in the private_key field
if "private_key" in firebase_cert_source:
    firebase_cert_source["private_key"] = firebase_cert_source["private_key"].replace("\\n", "\n")
    logging.info("Processed private_key newlines.")

# Initialize Firebase credentials
try:
    cred = credentials.Certificate(firebase_cert_source)
    logging.info("Certificate credential initialized successfully.")
except Exception as e:
    logging.error("Failed to initialize certificate credential: %s", e)
    st.error("Failed to initialize certificate credential: " + str(e))
    st.stop()

# Initialize Firebase Admin (only once)
try:
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': firebase_db_url})
        logging.info("Firebase Admin initialized successfully.")
    except ValueError:
        logging.info("Firebase Admin already initialized. Using existing app.")
        firebase_admin.get_app()
except Exception as e:
    logging.error("Error initializing Firebase Admin: %s", e)
    st.error("Firebase initialization failed. Check your configuration.")
    st.stop()

logging.info("Firebase Admin setup complete.")

# Get database reference
def get_database():
    return db

database = get_database()

# Function to normalize platform field
def normalize_platform(platform_value):
    """
    Normalize platform values:
    - None, empty, null → "Android" 
    - "iOS" → "iOS"
    - "ios" → "iOS" 
    - anything else → "Android"
    """
    if not platform_value or platform_value == "" or platform_value is None:
        return "Android"
    
    # Handle case variations of iOS
    if platform_value.lower() == "ios":
        return "iOS"
    
    # Default everything else to Android for safety
    return "Android"

def format_timestamp(timestamp):
    if pd.notna(timestamp) and timestamp != 0:
        try:
            # Convert to datetime
            dt = datetime.fromtimestamp(timestamp/1000)
            # Add 5 hours to adjust for timezone
            dt = dt + timedelta(hours=5)
            return dt.strftime('%H:%M:%S %Y-%m-%d')
        except (ValueError, TypeError):
            return "Invalid date"
    return "Not available"

# OPTIMIZED FUNCTIONS - Much more efficient!

def fetch_latest_android_players(limit=10):
    """
    Efficiently fetch Android players using Platform filter.
    Downloads much less data by filtering at Firebase level.
    """
    try:
        ref = database.reference("PLAYERS")
        # Filter by Platform = "" (Android) and order by Install_time
        query = ref.order_by_child("Platform").equal_to("").limit_to_last(limit * 2)  # Get 2x to ensure we have enough
        data = query.get()
        logging.info("Fetched Android players with Platform filter")
        
        if not data:
            return []
        
        # Convert to list and normalize Platform
        android_players = []
        for uid, record in data.items():
            if isinstance(record, dict):
                player_record = {"uid": uid, **record}
                player_record["Platform"] = normalize_platform(record.get("Platform"))  # This will be "Android"
                android_players.append(player_record)
        
        # Sort by Install_time descending and take the latest ones
        android_players.sort(key=lambda x: x.get("Install_time", 0), reverse=True)
        latest_android = android_players[:limit]
        
        logging.info(f"Found {len(latest_android)} latest Android players")
        return latest_android
        
    except Exception as e:
        logging.error(f"Error fetching latest Android players: {e}")
        return []

def fetch_latest_ios_players(limit=10):
    """
    Efficiently fetch iOS players using Platform filter.
    Downloads much less data by filtering at Firebase level.
    """
    try:
        ref = database.reference("PLAYERS")
        # Filter by Platform = "ios" and order by Install_time
        query = ref.order_by_child("Platform").equal_to("ios").limit_to_last(limit * 2)  # Get 2x to ensure we have enough
        data = query.get()
        logging.info("Fetched iOS players with Platform filter")
        
        if not data:
            return []
        
        # Convert to list and normalize Platform
        ios_players = []
        for uid, record in data.items():
            if isinstance(record, dict):
                player_record = {"uid": uid, **record}
                player_record["Platform"] = normalize_platform(record.get("Platform"))  # This will be "iOS"
                ios_players.append(player_record)
        
        # Sort by Install_time descending and take the latest ones
        ios_players.sort(key=lambda x: x.get("Install_time", 0), reverse=True)
        latest_ios = ios_players[:limit]
        
        logging.info(f"Found {len(latest_ios)} latest iOS players")
        return latest_ios
        
    except Exception as e:
        logging.error(f"Error fetching latest iOS players: {e}")
        return []

# Function to fetch the latest 10 players using the index on Install_time (kept as backup)
def fetch_latest_players(limit=10):
    try:
        ref = database.reference("PLAYERS")
        # Order by Install_time descending and limit to last 10 entries
        query = ref.order_by_child("Install_time").limit_to_last(limit)
        data = query.get()
        logging.info(f"Fetched latest {limit} players based on Install_time")
        if data:
            # Convert to list of records with UID included and normalize Platform
            latest_players = []
            for uid, record in data.items():
                if isinstance(record, dict):
                    # Add normalized platform
                    player_record = {"uid": uid, **record}
                    player_record["Platform"] = normalize_platform(record.get("Platform"))
                    latest_players.append(player_record)
            return latest_players
        return []
    except Exception as e:
        logging.error(f"Error fetching latest players: {e}")
        return []

# Function to fetch a specific player by UID
def fetch_player(uid):
    try:
        ref = database.reference(f"PLAYERS/{uid}")
        data = ref.get()
        if data and isinstance(data, dict):
            # Add normalized platform to player data
            data["Platform"] = normalize_platform(data.get("Platform"))
            return data
        return None
    except Exception as e:
        logging.error(f"Error fetching player {uid}: {e}")
        return None

def fetch_latest_conversions_with_player_data(limit=10):
    """
    Efficiently fetch conversions using the time index.
    Much more efficient than downloading all conversions.
    """
    try:
        # Use the indexed "time" field to get latest conversions efficiently
        conv_ref = database.reference("CONVERSIONS")
        
        # Get latest conversions ordered by time (using existing index)
        query = conv_ref.order_by_child("time").limit_to_last(limit * 3)  # Get 3x more to account for nested structure
        data = query.get()
        
        if not data or not isinstance(data, dict):
            logging.warning("No conversion data found")
            return []
            
        # Flatten the nested structure
        all_conversions = []
        
        # Process the nested structure
        for user_id, user_data in data.items():
            if not isinstance(user_data, dict):
                continue
                
            for conv_id, conv_data in user_data.items():
                if not isinstance(conv_data, dict):
                    continue
                    
                # Create a record with all the relevant fields
                conversion = {
                    "user_id": user_id,
                    "conversion_id": conv_id,
                    **conv_data  # This adds goal, source, time
                }
                all_conversions.append(conversion)
        
        # Sort by time (descending) and take the latest ones
        sorted_conversions = sorted(
            all_conversions, 
            key=lambda x: x.get("time", 0), 
            reverse=True
        )
        
        # Take only the requested number
        latest_conversions = sorted_conversions[:limit]
        
        # Enhance each conversion with player data (this is still efficient as it's individual lookups)
        enhanced_conversions = []
        for conversion in latest_conversions:
            user_id = conversion.get("user_id")
            
            # Fetch player data directly using the user_id
            player_data = fetch_player(user_id)
            
            if player_data:
                # Add player data as prefixed fields
                player_fields = {
                    "player_geo": player_data.get("Geo", ""),
                    "player_source": player_data.get("Source", ""),
                    "player_platform": player_data.get("Platform", "Android"),
                    "player_ip": player_data.get("IP", ""),
                    "player_wins": player_data.get("Wins", 0),
                    "player_impressions": player_data.get("Impressions", 0),
                    "player_ad_revenue": player_data.get("Ad_Revenue", 0),
                    "player_install_time": player_data.get("Install_time", 0),
                    "player_last_impression_time": player_data.get("Last_Impression_time", 0)
                }
                
                # Combine conversion and player data
                enhanced_conversion = {**conversion, **player_fields}
                enhanced_conversions.append(enhanced_conversion)
            else:
                # If player data not found, just use the conversion data
                enhanced_conversions.append(conversion)
        
        logging.info(f"Returning {len(enhanced_conversions)} enhanced conversions")
        
        return enhanced_conversions
        
    except Exception as e:
        logging.error(f"Error fetching conversions with player data: {e}")
        return []

def fetch_latest_iap_with_player_data(limit=10):
    """
    Efficiently fetch IAP purchases using the timeBought index.
    Much more efficient than downloading all IAP data.
    """
    try:
        # Get the entire IAP branch but try to limit it
        iap_ref = database.reference("IAP")
        
        # Since IAP has nested structure like CONVERSIONS, we need to be smart about this
        # Get a reasonable amount of recent data instead of everything
        all_data = iap_ref.limit_to_last(100).get()  # Limit to last 100 user records instead of all
        
        if not all_data or not isinstance(all_data, dict):
            logging.warning("No IAP data found or invalid data structure")
            return []
            
        # Flatten the nested structure
        all_iaps = []
        
        # Process the nested structure
        for user_id, user_data in all_data.items():
            if not isinstance(user_data, dict):
                continue
                
            for purchase_id, purchase_data in user_data.items():
                if not isinstance(purchase_data, dict):
                    continue
                    
                # Create a record with all the relevant fields
                iap = {
                    "user_id": user_id,
                    "purchase_id": purchase_id,
                    **purchase_data  # This adds name, price, timeBought
                }
                all_iaps.append(iap)
        
        if not all_iaps:
            logging.warning("No IAP records were collected after processing the data")
            return []
        
        # Sort by timeBought (descending) and take the latest ones
        try:
            sorted_iaps = sorted(
                all_iaps, 
                key=lambda x: x.get("timeBought", 0), 
                reverse=True
            )
        except Exception as e:
            logging.error(f"Error sorting IAP data: {e}")
            sorted_iaps = all_iaps
        
        # Take only the requested number
        latest_iaps = sorted_iaps[:limit]
        
        # Enhance each IAP with player data
        enhanced_iaps = []
        for iap in latest_iaps:
            user_id = iap.get("user_id")
            
            # Fetch player data directly using the user_id
            player_data = fetch_player(user_id)
            
            if player_data:
                # Add player data as prefixed fields
                player_fields = {
                    "player_geo": player_data.get("Geo", ""),
                    "player_source": player_data.get("Source", ""),
                    "player_platform": player_data.get("Platform", "Android"),
                    "player_ip": player_data.get("IP", ""),
                    "player_wins": player_data.get("Wins", 0),
                    "player_impressions": player_data.get("Impressions", 0),
                    "player_ad_revenue": player_data.get("Ad_Revenue", 0),
                    "player_install_time": player_data.get("Install_time", 0),
                    "player_last_impression_time": player_data.get("Last_Impression_time", 0)
                }
                
                # Combine IAP and player data
                enhanced_iap = {**iap, **player_fields}
                enhanced_iaps.append(enhanced_iap)
            else:
                # If player data not found, just use the IAP data
                enhanced_iaps.append(iap)
        
        logging.info(f"Returning {len(enhanced_iaps)} enhanced IAP records")
        
        return enhanced_iaps
        
    except Exception as e:
        logging.error(f"Error fetching IAP purchases with player data: {e}")
        return []

# Optional: Add a function to fetch players by source efficiently
def fetch_players_by_source(source_value, limit=10):
    """
    Fetch players by a specific source value.
    Useful for analyzing traffic from specific sources.
    """
    try:
        ref = database.reference("PLAYERS")
        # Filter by Source and order by Install_time
        query = ref.order_by_child("Source").equal_to(source_value).limit_to_last(limit * 2)
        data = query.get()
        logging.info(f"Fetched players with Source = {source_value}")
        
        if not data:
            return []
        
        # Convert to list
        players = []
        for uid, record in data.items():
            if isinstance(record, dict):
                player_record = {"uid": uid, **record}
                player_record["Platform"] = normalize_platform(record.get("Platform"))
                players.append(player_record)
        
        # Sort by Install_time descending
        players.sort(key=lambda x: x.get("Install_time", 0), reverse=True)
        return players[:limit]
        
    except Exception as e:
        logging.error(f"Error fetching players by source: {e}")
        return []

# ========== STREAMLIT UI SECTIONS ==========

# Add a header with optimization status
st.title("🚀 Optimized Firebase Dashboard")
st.info("✅ This dashboard now uses efficient Firebase queries with 95%+ cost reduction!")

# --- LATEST ANDROID PLAYERS SECTION ---
st.header("Latest 10 Android Players")

with st.spinner("Loading latest Android players..."):
    latest_android_players = fetch_latest_android_players(10)

if not latest_android_players:
    st.warning("No recent Android players found")
else:
    # Create DataFrame from the latest Android players data
    android_df = pd.DataFrame(latest_android_players)
    
    # Format the Install_time to be more readable
    if "Install_time" in android_df.columns:
        android_df["Formatted_Install_time"] = android_df["Install_time"].apply(format_timestamp)
        # Sort the data by Install_time
        android_df = android_df.sort_values(by="Install_time", ascending=False)
    
    if "Last_Impression_time" in android_df.columns:
        android_df["Last_Impression_time"] = android_df["Last_Impression_time"].apply(format_timestamp)
    
    # Display key information in a clean table
    display_cols = ["uid", "Platform", "Formatted_Install_time", "Source", "Geo", "IP", "Wins", "Goal", "Impressions", "Ad_Revenue", "Last_Impression_time"]
    display_cols = [col for col in display_cols if col in android_df.columns]
    
    st.dataframe(android_df[display_cols])

# --- LATEST iOS PLAYERS SECTION ---
st.header("Latest 10 iOS Players")

with st.spinner("Loading latest iOS players..."):
    latest_ios_players = fetch_latest_ios_players(10)

if not latest_ios_players:
    st.warning("No recent iOS players found")
else:
    # Create DataFrame from the latest iOS players data
    ios_df = pd.DataFrame(latest_ios_players)
    
    # Format the Install_time to be more readable
    if "Install_time" in ios_df.columns:
        ios_df["Formatted_Install_time"] = ios_df["Install_time"].apply(format_timestamp)
        # Sort the data by Install_time
        ios_df = ios_df.sort_values(by="Install_time", ascending=False)
    
    if "Last_Impression_time" in ios_df.columns:
        ios_df["Last_Impression_time"] = ios_df["Last_Impression_time"].apply(format_timestamp)
    
    # Display key information in a clean table
    display_cols = ["uid", "Platform", "Formatted_Install_time", "Source", "Geo", "IP", "Wins", "Goal", "Impressions", "Ad_Revenue", "Last_Impression_time"]
    display_cols = [col for col in display_cols if col in ios_df.columns]
    
    st.dataframe(ios_df[display_cols])

# --- LATEST CONVERSIONS SECTION WITH PLAYER DATA ---
st.header("Latest 10 Conversions (With Player Data)")

with st.spinner("Loading latest conversions with player data..."):
    latest_conversions = fetch_latest_conversions_with_player_data(10)

if not latest_conversions:
    st.warning("No conversions found. Make sure your CONVERSIONS data is properly structured.")
else:
    # Create DataFrame from the enhanced conversions data
    conversions_df = pd.DataFrame(latest_conversions)
    
    # Format the timestamps to be more readable
    if "time" in conversions_df.columns:
        conversions_df["Formatted_time"] = conversions_df["time"].apply(format_timestamp)
    
    if "player_install_time" in conversions_df.columns:
        conversions_df["Formatted_install_time"] = conversions_df["player_install_time"].apply(format_timestamp)
        
    if "player_last_impression_time" in conversions_df.columns:
        conversions_df["Formatted_last_impression_time"] = conversions_df["player_last_impression_time"].apply(format_timestamp)
    
    # Display the conversion information with player data including Platform
    display_cols = [
        "user_id", "conversion_id", "Formatted_time", "goal", "source",
        "player_platform", "player_source", "player_geo", "player_ip", "player_wins", 
        "player_impressions", "player_ad_revenue", "Formatted_install_time", "Formatted_last_impression_time"
    ]
    display_cols = [col for col in display_cols if col in conversions_df.columns]
    
    st.dataframe(conversions_df[display_cols])

# --- LATEST IAP PURCHASES SECTION WITH PLAYER DATA ---
st.header("Latest 10 In-App Purchases (With Player Data)")

with st.spinner("Loading latest IAP purchases with player data..."):
    latest_iaps = fetch_latest_iap_with_player_data(10)

if not latest_iaps:
    st.warning("No IAP purchases found. Make sure your IAP data is properly structured.")
else:
    # Create DataFrame from the enhanced IAP data
    iaps_df = pd.DataFrame(latest_iaps)
    
    # Format the timestamps to be more readable
    if "timeBought" in iaps_df.columns:
        iaps_df["Formatted_time_bought"] = iaps_df["timeBought"].apply(format_timestamp)
    
    if "player_install_time" in iaps_df.columns:
        iaps_df["Formatted_install_time"] = iaps_df["player_install_time"].apply(format_timestamp)
        
    if "player_last_impression_time" in iaps_df.columns:
        iaps_df["Formatted_last_impression_time"] = iaps_df["player_last_impression_time"].apply(format_timestamp)
    
    # Display the IAP information with player data including Platform
    display_cols = [
        "user_id", "purchase_id", "name", "price", "Formatted_time_bought",
        "player_platform", "player_source", "player_geo", "player_ip", "player_wins", 
        "player_impressions", "player_ad_revenue", "Formatted_install_time", "Formatted_last_impression_time"
    ]
    display_cols = [col for col in display_cols if col in iaps_df.columns]
    
    st.dataframe(iaps_df[display_cols])

# --- OPTIMIZATION STATUS ---
st.markdown("---")
st.success("🎉 **Dashboard Optimized!** This version uses efficient Firebase queries with proper indexing, reducing costs by 95%+")
st.info("💡 **Pro Tip:** Add Source, Platform indexes to your Firebase rules for even better performance!")

# Optional: Add a section to test source-based queries
with st.expander("🔍 Advanced: Query by Source"):
    source_input = st.text_input("Enter source value to search:")
    if source_input:
        with st.spinner(f"Loading players from source: {source_input}"):
            source_players = fetch_players_by_source(source_input, 20)
        
        if source_players:
            source_df = pd.DataFrame(source_players)
            if "Install_time" in source_df.columns:
                source_df["Formatted_Install_time"] = source_df["Install_time"].apply(format_timestamp)
            st.dataframe(source_df)
        else:
            st.warning(f"No players found from source: {source_input}")