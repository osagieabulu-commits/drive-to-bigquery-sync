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
    'avr__poa_': {'id_col': 'account_number', 'date_col': 'approval_date'},
    'avr__auto_review_': {'id_col': 'account_number', 'date_col': 'date'},
}

# Safety circuit breaker for upsert reconciliation: if more rows than this
# fraction look "removed from source" in a single run, treat it as a likely
# partial/failed download rather than real removals, and skip cleanup for
# that run rather than risk mass-deleting still-valid rows.
UPSERT_SAFETY_MAX_REMOVAL_RATIO = 0.5

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
    
    # --- GATEKEEPER: Only allow valid data files (.csv, .xlsx, .xls, Google Sheets) ---
    is_valid_file = (
        'csv' in mime_type or 
        'spreadsheet' in mime_type or 
        'excel' in mime_type or 
        'openxmlformats' in mime_type or 
        file_name.lower().endswith('.xlsx') or 
        file_name.lower().endswith('.xls')
    )
    if not is_valid_file:
        return None

    try:
        request = service.files().export_media(fileId=file_id, mimeType='text/csv') if mime_type == 'application/vnd.google-apps.spreadsheet' else service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        
        # Parse based on file type
        if 'csv' in mime_type or mime_type == 'application/vnd.google-apps.spreadsheet' or file_name.lower().endswith('.csv'):
            try:
                df = pd.read_csv(fh, low_memory=False, dtype=str, keep_default_na=False, encoding='utf-8')
            except UnicodeDecodeError:
                # Fallback if the CSV has weird text encoding
                fh.seek(0)
                df = pd.read_csv(fh, low_memory=False, dtype=str, keep_default_na=False, encoding='latin1')
        else: 
            # Pandas will auto-detect between openpyxl (.xlsx) and xlrd (.xls)
            df = pd.read_excel(fh, dtype=str)
            
        if df.empty: return None
        df.columns = [re.sub(r'[\s\W]+', '_', str(col).strip().lower()) for col in df.columns]
        return df.astype(str).replace(['nan', 'NaN', 'None', '', 'NULL', '<NA>', 'nat', 'NaT'], None)
    except Exception as e:
        print(f"❌ Error downloading {file_name}: {e}")
        return None

def _normalize_id_column(series):
    """Coerce an ID column to clean, comparable strings regardless of whether
    it arrived as a BigQuery-typed numeric column (existing_df) or a raw
    string from Drive (df). Without this, the same id — e.g. an
    account_number that BQ autodetected as INTEGER/FLOAT — gets compared as
    1000123456 vs '1000123456' and drop_duplicates treats them as two
    different rows, so the old (often still-"pending") copy never gets
    collapsed or overwritten by the newer one.
    """
    def clean(v):
        if pd.isna(v):
            return None
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        cleaned = str(v).strip()
        return cleaned if cleaned else None
    return series.apply(clean)

def perform_upsert_free_tier(df, table_name, bq_client, id_col, date_col):
    """Universal Upsert logic for Free Tier with Index Fix."""
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

    df[id_col] = _normalize_id_column(df[id_col])

    # Upsert tables always re-download every file in their folder each run
    # (see main()), so df is the FULL current state of the source for this
    # table right now. That means any id_col present in BQ but missing from
    # df has been resolved/archived/removed upstream — it should NOT keep
    # being carried forward as "still pending" forever.
    current_ids = set(df[id_col].dropna())

    try:
        print(f"📥 Pulling existing data for {table_name} upsert...")
        # Download existing data
        existing_df = bq_client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        existing_df[id_col] = _normalize_id_column(existing_df[id_col])

        removed_mask = ~existing_df[id_col].isin(current_ids)
        removed_count = int(removed_mask.sum())
        removal_ratio = removed_count / len(existing_df) if len(existing_df) else 0

        if removal_ratio > UPSERT_SAFETY_MAX_REMOVAL_RATIO:
            # More likely a partial/failed download than a genuine wave of
            # resolutions — skip cleanup this run rather than risk deleting
            # rows that are still valid in the source.
            print(f"⚠️ {removed_count}/{len(existing_df)} rows for {table_name} look removed from "
                  f"source (>{UPSERT_SAFETY_MAX_REMOVAL_RATIO:.0%}) — skipping stale-row cleanup this run as a precaution.")
        elif removed_count:
            print(f"🗑️ {removed_count} rows no longer present in source for {table_name} — removing stale records.")
            existing_df = existing_df[~removed_mask]

        # Combine data and ignore index to prevent __index_level_0__ duplication
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        
        # Ensure date column is datetime for correct sorting. na_position='first'
        # sends blank/unparseable dates to the FRONT of the sort, so a row with
        # no date can never be mistaken for "the latest" — previously, blank
        # dates sorted LAST by default, so a still-pending row with no date
        # could outrank a genuinely newer, dated row under keep='last'.
        combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors='coerce')
        
        # Sort by date and keep the latest record for each ID.
        # kind='mergesort' is a stable sort: when dates tie exactly (including
        # ties among several blank ones), the row from THIS run's fresh pull —
        # concatenated after existing_df — reliably wins.
        final_df = combined_df.sort_values(
            date_col, ascending=True, na_position='first', kind='mergesort'
        ).drop_duplicates(subset=[id_col], keep='last')
        
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
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
        
        # --- NEW: SELF-HEALING CHECK ---
        table_exists = True
        try:
            bq_client.get_table(table_id)
        except Exception:
            table_exists = False
            print(f"\n📢 Table '{table_name}' is missing in BQ! Forcing full refresh.")
        # -------------------------------

        files = drive_service.files().list(
            q=f"'{folder['id']}' in parents and trashed = false", 
            fields="files(id, name, mimeType, modifiedTime)"
        ).execute().get('files', [])
        
        dfs_to_process = []
        for f in files:
            file_id = f['id']
            
            # Upsert tables ALWAYS download. 
            # Standard tables check cache, BUT ignore cache if the table was deleted in BQ
            if table_name not in UPSERT_TABLES:
                if table_exists and state.get(file_id) == f['modifiedTime']: 
                    continue
            
            df = download_file_to_dataframe(drive_service, f)
            if df is not None:
                print(f"📄 Downloaded: {f['name']}")
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
