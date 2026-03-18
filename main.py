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
JIRA_TABLE_NAME = 'jira_timepiece'

# Standard Tables: Only add NEW rows (File-level skipping enabled)
TABLE_KEY_COLUMNS = {
    'ph_dates': ['date'],
    'k4b_sla_table': ['conversation_id', 'sla_started_at_africa_algiers_'],
    'retail_sla_table': ['conversation_id', 'sla_started_at_africa_algiers_'],
    'avr_cbn_remediation': ['account_number', 'physical_address_verification_time'],
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
    'avr__poa_': ['account_number', 'created_at'],
    'avr__house_street_view_': ['account_number', 'created_at'],
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
# --- STATE MANAGEMENT ---
# ==============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# ==============================================================================
# --- CORE LOGIC ---
# ==============================================================================

def download_file_to_dataframe(service, file_info):
    file_id, file_name, mime_type = file_info['id'], file_info['name'], file_info['mimeType']
    try:
        if mime_type == 'application/vnd.google-apps.spreadsheet':
            request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        else:
            request = service.files().get_media(fileId=file_id)

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
        df = df.astype(str).replace(['nan', 'NaN', 'None', '', 'NULL', '<NA>', 'nat', 'NaT'], None)
        return df
    except Exception as e:
        print(f"❌ Error downloading {file_name}: {e}")
        return None

def perform_jira_upsert_free_tier(df, table_name, bq_client):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    try:
        existing_df = bq_client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df['updated'] = pd.to_datetime(combined_df['updated'], errors='coerce')
        combined_df = combined_df.sort_values('updated', ascending=True)
        final_df = combined_df.drop_duplicates(subset=['key'], keep='last')
        final_df = final_df.astype(str).replace(['nan', 'NaN', 'None', 'NaT', 'nat', ''], None)
    except:
        final_df = df

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config).result()
    return len(final_df)

def upload_standard(df, table_name, bq_client):
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

    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
    bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    return len(df)

def main():
    print("🚀 Starting Memory-Optimized Sync...")
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
            # JIRA logic ALWAYS processes files (to ensure updates are caught)
            # Standard logic checks if file has changed
            if table_name != JIRA_TABLE_NAME:
                if state.get(file_id) == f['modifiedTime']:
                    continue # SKIP DOWNLOADING
            
            print(f"📄 Downloading: {f['name']}")
            df = download_file_to_dataframe(drive_service, f)
            if df is not None:
                dfs_to_process.append(df)
                state[file_id] = f['modifiedTime'] # Record that we processed this version

        if dfs_to_process:
            combined_df = pd.concat(dfs_to_process, ignore_index=True)
            if table_name == JIRA_TABLE_NAME:
                rows = perform_jira_upsert_free_tier(combined_df, table_name, bq_client)
                print(f"✅ Jira Upsert: {rows} rows in BQ.")
            else:
                rows = upload_standard(combined_df, table_name, bq_client)
                print(f"✅ {table_name}: Added {rows} rows.")
        else:
            print(f"⏭️ No new/modified files for {table_name}.")

    save_state(state)
    print("🎉 Sync completed!")

if __name__ == '__main__':
    main()
