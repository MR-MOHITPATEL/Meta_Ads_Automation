import os
import gspread
from google.oauth2.service_account import Credentials
from src.utils import setup_logger, retry

logger = setup_logger("sheets_api")

class GoogleSheetsClient:
    def __init__(self, spreadsheet_id=None, sheet_name="Daily Insights", test_mode=False):
        self.credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials/sa.json")
        self.spreadsheet_id = spreadsheet_id or os.getenv("GOOGLE_SPREADSHEET_ID")
        self.test_mode = test_mode
        self.sheet_name = "meta_hourly_test" if test_mode else sheet_name
        
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        self.client = self._authenticate()
        
        if not self.spreadsheet_id:
            logger.info("GOOGLE_SPREADSHEET_ID is missing! Creating a new Google Spreadsheet dynamically...")
            new_ss = self.client.create("Meta Ads Pipeline Outputs")
            self.spreadsheet_id = new_ss.id
            logger.info(f"Successfully created a new Google Spreadsheet. ID: {self.spreadsheet_id}")
            with open('.env', 'a') as f:
                f.write(f"\nGOOGLE_SPREADSHEET_ID={self.spreadsheet_id}\n")
            
        self.sheet = self._get_or_create_sheet()

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def clear_all_rows(self):
        """
        Clears all data from the current worksheet except for the header row.
        Used primarily for clean test runs.
        """
        logger.info(f"Clearing all rows from sheet: {self.sheet_name}")
        # Clear everything from row 2 onwards
        self.sheet.batch_clear(["A2:Q1000"]) 

    def _authenticate(self):
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if creds_json:
            import json
            creds_dict = json.loads(creds_json)
            credentials = Credentials.from_service_account_info(creds_dict, scopes=self.scopes)
        elif os.path.exists(self.credentials_file):
            credentials = Credentials.from_service_account_file(self.credentials_file, scopes=self.scopes)
        else:
            raise ValueError("Google credentials missing.")
            
        return gspread.authorize(credentials)

    @retry(Exception, tries=4, delay=5, backoff=2, logger=logger)
    def _get_or_create_sheet(self):
        spreadsheet = self.client.open_by_key(self.spreadsheet_id)
        expected_headers = [
            "campaign_name", "date", "week_of_month", "spend", "cpm", "cpc", "ctr", 
            "link_clicks", "web_page_views", "click_to_view_ratio", "cpt", "revenue", "roas", "atc",
            "impressions", "extraction_hour"
        ]
        
        try:
            worksheet = spreadsheet.worksheet(self.sheet_name)
            # CHECK HEADERS SYNC
            existing_headers = worksheet.row_values(1)
            if existing_headers != expected_headers:
                logger.info("Schema mismatch detected! Updating headers...")
                # Note: This is an aggressive sync for the refinement phase
                # We overwrite the first row. 
                worksheet.update('A1', [expected_headers])
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Worksheet '{self.sheet_name}' not found. Creating it...")
            worksheet = spreadsheet.add_worksheet(title=self.sheet_name, rows="1000", cols="20")
            worksheet.append_row(expected_headers)
        return worksheet

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def get_existing_keys_with_index(self):
        """
        Fetches all data in one call to map (Name, Date) to row indices.
        """
        all_data = self.sheet.get_all_values()
        if not all_data:
            return {}
            
        header_row = all_data[0]
        try:
            name_idx = header_row.index("campaign_name")
            date_idx = header_row.index("date")
        except ValueError:
            logger.warning("Required columns for matching (campaign_name, date) not found!")
            return {}
            
        existing_map = {}
        for row_idx, row in enumerate(all_data):
            if row_idx == 0: continue # Skip header
            if len(row) > max(name_idx, date_idx):
                key = f"{row[name_idx]}_{row[date_idx]}"
                existing_map[key] = row_idx + 1 # GSpread uses 1-based indexing
            
        return existing_map

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def upsert_rows(self, data_list):
        """
        Idempotent UPSERT: Updates rows if campaign_name+date exists. Inserts new if they don't.
        In test_mode, it clears the sheet first.
        """
        if not data_list:
            logger.info("No data to upsert.")
            return 0, 0
            
        if self.test_mode:
            self.clear_all_rows()
            existing_map = {}
        else:
            existing_map = self.get_existing_keys_with_index()
        
        rows_to_insert = []
        updates = []
        
        # Determine column letters for range updates, schema is 16 columns (A -> P)
        for row in data_list:
            # key is (Name, Date) -> row[0], row[1]
            key = f"{row[0]}_{row[1]}"
            
            if key in existing_map:
                row_idx = existing_map[key]
                updates.append({
                    'range': f'A{row_idx}:P{row_idx}', 
                    'values': [row]
                })
            else:
                rows_to_insert.append(row)
                existing_map[key] = -1 
                
        # 1. Execute Batch Update
        if updates:
            self.sheet.batch_update(updates)
            
        # 2. Execute Batch Insert
        if rows_to_insert:
            self.sheet.append_rows(rows_to_insert)
            
        # 3. Sort the sheet by Date (Col 2) Ascending
        try:
            self.sheet.sort((2, 'asc'))
        except Exception as e:
            logger.warning(f"Failed to sort sheet: {e}")

        logger.info("-" * 30)
        logger.info(f"Total processed: {len(data_list)}")
        logger.info(f"Rows updated:   {len(updates)}")
        logger.info(f"Rows inserted:  {len(rows_to_insert)}")
        logger.info("-" * 30)
            
        return len(rows_to_insert), len(updates)
