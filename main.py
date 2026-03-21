import os
import io
import json
import pandas as pd
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
MAIN_FOLDER_ID = '1Bmvq-3vm1L1UWiZPwIFc5yVdt6eYC2M6'
BQ_PROJECT_ID = 'lead-db-drive-bigquery'
BQ_DATASET_ID = 'Leadership_dashboard'
STATE_FILE = 'sync_state.json'

# --- UPSERT CONFIGURATION ---
# These tables will look at existing data in BQ, merge with Drive, 
# and keep only the latest record based on the date column.
UPSERT_TABLES = {
    'jira_timepiece': {'id_col': 'key', 'date_col': 'updated'},
    'avr__house_street_view_': {'id_col': 'account_number', 'date_col': 'approval_date'},
    'avr__poa_': {'id_col': 'account_number', 'date_col': 'approval_date'}
}

# Standard Tables: Only add NEW rows (Incremental via Key check)
TABLE_KEY_COLUMNS = {
    'ph_dates': ['date'],
    'k4b_sla_table': ['conversation_id', 'sla_started_at_africa_algiers_'],
    'retail_sla_table': ['conversation_id', 'sla_started_at_africa_algiers_'],
    'account_closure': ['account_number', 'date_of_request'],
    'cx_scroe_rating_topics': ['conversation_id', 'conversation_last_closed_at_africa_algiers_'],
    'retail_reopened_conv_source': ['conversation_id', 'action_time_africa_algiers_'],
    'k4b_reopened_conv_source': ['conversation_id', 'action_time_africa_algiers_'],
    'qa_scores': ['ticket_id', 'ticket_started_date'],
    'k4b_tickets_source': ['conversation_id', 'ticket_created_africa_algiers_'],
    'k4b_conv_source': ['conversation_id', 'conversation_started_at_africa_algiers_'],
    'k4b_contact_reasons': ['conversation_id', 'conversation_started_at_africa_algiers_'],
    'retail_conv_contact_reasons': ['conversation_id', 'conversation_started_at_africa_algiers_', 'ticket_created_africa_algiers_', 'conversation_created_at_africa_algiers_'],
    'retail_conv_source': ['conversation_id', 'conversation_started_at_africa_algiers_'],
    'retail_tickets_source': ['conversation_id', 'ticket_created_africa_algiers_'],
    'voice_inbound_calls_source': ['uniqueid', 'starttime'],
    'voice_outbound_calls_source': ['uniqueid', 'lastcallat'],
    'voice_csat': ['caller_id', 'date_time_of_call'],
    'k4b_upgrages': ['id', 'creation_date'],
    'k4b_upgrade__freelance_': ['customerid', 'created_at'],
    'avr__manual_': ['account_number', 'creation_date'],
    'avr__auto_review_': ['account_number', 'date'],
    'fcr_subcat_source': ['conversation_id', 'conversation_started_at_africa_algiers_'],
}

# Load Credentials
GCP_JSON = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
SERVICE_ACCOUNT_INFO = json.loads(GCP_JSON)
CREDS = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
BQ_CREDS = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)

# ==============================================================================
# --- LOGIC FUNCTIONS ---
# ==============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f: return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f: json.dump(state, f)

def download_file_to_dataframe(service, file_info):
    file_id, file_name, mime_type = file_info['id'], file_info['name'], file_info['mimeType']
    try:
        request = service.files().export_media(fileId=file_id, mimeType='text/csv') if mime_type == 'application/vnd.google-apps.spreadsheet' else service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        
        if mime_type in ['text/csv', 'application/vnd.google-apps.spreadsheet']:
            df = pd.read_csv(fh, low_memory=False, dtype=str, keep_default_na=False)
        else: 
            df = pd.read_excel(fh, dtype=str)
            
        if df.empty: return None
        df.columns = [re.sub(r'[\s\W]+', '_', str(col).strip().lower()) for col in df.columns]
        return df.astype(str).replace(['nan', 'NaN', 'None', '', 'NULL', '<NA>', 'nat', 'NaT'], None)
    except Exception as e:
        print(f"❌ Error downloading {file_name}: {e}")
        return None

def perform_upsert_free_tier(df, table_name, bq_client, id_col, date_col):
    """Universal Upsert logic for Free Tier with Index Fix."""
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    try:
        print(f"📥 Pulling existing data for {table_name} upsert...")
        # Download existing data
        existing_df = bq_client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        
        # Combine data and ignore index to prevent __index_level_0__ duplication
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        
        # Ensure date column is datetime for correct sorting
        combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors='coerce')
        
        # Sort by date and keep the latest record for each ID
        final_df = combined_df.sort_values(date_col, ascending=True).drop_duplicates(subset=[id_col], keep='last')
        
        # CRITICAL: Reset index and drop it so it doesn't get sent to BigQuery as a column
        final_df = final_df.reset_index(drop=True)
        
        # Clean up types for BigQuery
        final_df = final_df.astype(str).replace(['nan', 'NaN', 'None', 'NaT', 'nat', ''], None)
        
    except Exception as e:
        print(f"ℹ️ Starting fresh or error on {table_name}: {e}")
        final_df = df.reset_index(drop=True)

    # Use WRITE_TRUNCATE to replace the table with the merged/deduplicated version
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config).result()
    return len(final_df)

def upload_standard(df, table_name, bq_client):
    """Standard Incremental logic for non-upsert tables."""
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    keys = TABLE_KEY_COLUMNS.get(table_name)
    try:
        bq_client.get_table(table_id)
        if keys:
            key_select = ", ".join([f"CAST(`{k}` AS STRING)" for k in keys])
            query = f"SELECT DISTINCT CONCAT({key_select}) as row_id FROM `{table_id}`"
            existing_ids = set(bq_client.query(query).to_dataframe()['row_id'])
            
            df['_tmp_id'] = ""
            for k in keys: df['_tmp_id'] += df[k].fillna('NULL').astype(str)
            df = df[~df['_tmp_id'].isin(existing_ids)].drop(columns=['_tmp_id'])
            
            if df.empty: return 0
            write_disposition = "WRITE_APPEND"
        else: 
            write_disposition = "WRITE_TRUNCATE"
    except: 
        write_disposition = "WRITE_TRUNCATE"

    # Reset index here as well for safety
    df = df.reset_index(drop=True)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
    bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    return len(df)

def main():
    print("🚀 Starting Hybrid Memory-Optimized Sync...")
    drive_service = build('drive', 'v3', credentials=CREDS)
    bq_client = bigquery.Client(credentials=BQ_CREDS, project=BQ_PROJECT_ID)
    state = load_state()
    
    subfolders = drive_service.files().list(
        q=f"'{MAIN_FOLDER_ID}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false", 
        fields="files(id, name)"
    ).execute().get('files', [])

    for folder in subfolders:
        table_name = re.sub(r'[\s\W]+', '_', folder['name'].strip().lower())
        files = drive_service.files().list(
            q=f"'{folder['id']}' in parents and trashed = false", 
            fields="files(id, name, mimeType, modifiedTime)"
        ).execute().get('files', [])
        
        dfs_to_process = []
        for f in files:
            file_id = f['id']
            # Upsert tables ALWAYS download. Standard tables check the sync_state cache.
            if table_name not in UPSERT_TABLES:
                if state.get(file_id) == f['modifiedTime']: continue
            
            print(f"📄 Downloading: {f['name']}")
            df = download_file_to_dataframe(drive_service, f)
            if df is not None:
                dfs_to_process.append(df)
                state[file_id] = f['modifiedTime']

        if dfs_to_process:
            combined_df = pd.concat(dfs_to_process, ignore_index=True)
            try:
                if table_name in UPSERT_TABLES:
                    conf = UPSERT_TABLES[table_name]
                    rows = perform_upsert_free_tier(combined_df, table_name, bq_client, conf['id_col'], conf['date_col'])
                    print(f"✅ {table_name} Upsert Complete: {rows} total rows.")
                else:
                    rows = upload_standard(combined_df, table_name, bq_client)
                    print(f"✅ {table_name}: Added {rows} rows.")
            except Exception as e:
                print(f"❌ Error processing table {table_name}: {e}")
        else: 
            print(f"⏭️ No updates for {table_name}.")

    save_state(state)
    print("🎉 Sync completed!")

if __name__ == '__main__':
    main()
