import os
import io
import time
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

# Load Credentials from GitHub Secrets
GCP_JSON = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
if not GCP_JSON:
    raise ValueError("CRITICAL: GCP_SERVICE_ACCOUNT_JSON secret not found in GitHub!")

SERVICE_ACCOUNT_INFO = json.loads(GCP_JSON)
CREDS = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
BQ_CREDS = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)

# --- TABLE MAPPING ---
# Tables LISTED here = Incremental (Only adds new rows)
# Tables REMOVED from here = Full Re-upload (Wipes and replaces daily)
TABLE_KEY_COLUMNS = {
    'jira_sla_control': ['sla'],
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

# ==============================================================================
# --- CORE FUNCTIONS ---
# ==============================================================================

def get_drive_service():
    return build('drive', 'v3', credentials=CREDS)

def get_bq_client():
    return bigquery.Client(credentials=BQ_CREDS, project=BQ_PROJECT_ID)

def download_file_to_dataframe(service, file_info):
    file_id, file_name, mime_type = file_info['id'], file_info['name'], file_info['mimeType']
    try:
        if mime_type == 'application/vnd.google-apps.spreadsheet':
            request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        elif mime_type in ['text/csv', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
            request = service.files().get_media(fileId=file_id)
        else: return None

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        
        if mime_type in ['text/csv', 'application/vnd.google-apps.spreadsheet']:
            try: df = pd.read_csv(fh, low_memory=False, dtype=str, keep_default_na=False)
            except: 
                fh.seek(0)
                df = pd.read_csv(fh, low_memory=False, encoding='latin1', dtype=str, keep_default_na=False)
        else: 
            df = pd.read_excel(fh, dtype=str)

        if df.empty: return None
        
        df.columns = [re.sub(r'[\s\W]+', '_', str(col).strip().lower()) for col in df.columns]
        df = df.astype(str).replace(['nan', 'NaN', 'None', '', 'NULL', '<NA>', 'nat', 'NaT'], None)
        return df
    except Exception as e:
        print(f"❌ Error processing {file_name}: {e}")
        return None

def upload_logic(df, table_name, bq_client):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
    keys = TABLE_KEY_COLUMNS.get(table_name)
    write_disposition = "WRITE_APPEND"

    try:
        bq_client.get_table(table_id)
        # If keys exist in our mapping AND in the data, do Incremental
        if keys and all(k in df.columns for k in keys):
            key_select = ", ".join([f"CAST({k} AS STRING)" for k in keys])
            query = f"SELECT DISTINCT CONCAT({key_select}) as row_id FROM `{table_id}`"
            existing_ids = set(bq_client.query(query).to_dataframe()['row_id'])
            
            df['_tmp_id'] = ""
            for k in keys: df['_tmp_id'] += df[k].fillna('NULL').astype(str)
            df = df[~df['_tmp_id'].isin(existing_ids)].drop(columns=['_tmp_id'])
            
            if df.empty: return 0
        else:
            # Table is known, but no keys provided -> FULL RE-UPLOAD
            print(f"♻️ No keys for '{table_name}'. Switching to FULL RE-UPLOAD mode.")
            write_disposition = "WRITE_TRUNCATE"

    except Exception:
        # Table doesn't exist yet -> Create it via Truncate
        write_disposition = "WRITE_TRUNCATE"

    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    return len(df)

def main():
    print("🚀 Starting Hybrid Sync Process...")
    drive_service = get_drive_service()
    bq_client = get_bq_client()
    
    folder_req = drive_service.files().list(
        q=f"'{MAIN_FOLDER_ID}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name)"
    )
    subfolders = folder_req.execute().get('files', [])

    for folder in subfolders:
        folder_id, folder_name = folder['id'], folder['name']
        table_name = re.sub(r'[\s\W]+', '_', folder_name.strip().lower())
        
        file_req = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name, mimeType)"
        )
        file_results = file_req.execute().get('files', [])
        
        new_dataframes = []
        for f in file_results:
            print(f"📄 Processing: {f['name']}")
            df = download_file_to_dataframe(drive_service, f)
            if df is not None:
                new_dataframes.append(df)

        if new_dataframes:
            try:
                combined_df = pd.concat(new_dataframes, ignore_index=True)
                rows = upload_logic(combined_df, table_name, bq_client)
                print(f"✅ Table '{table_name}': Processed {rows} rows.")
            except Exception as e:
                print(f"❌ Failed folder '{folder_name}': {e}")
        else:
            print(f"⏭️ No data found for '{folder_name}'")

    print("🎉 Sync completed successfully!")

if __name__ == '__main__':
    main()
