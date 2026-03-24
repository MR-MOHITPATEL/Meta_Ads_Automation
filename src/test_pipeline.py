import os
import csv
from pprint import pprint
from src.utils import setup_logger
from src.meta_api import MetaAPIClient
from src.pipeline import transform_data

TEST_MODE = True
TEST_HOURS = 24
LOCAL_OUTPUT_FILE = "test_output.csv"
test_logger = setup_logger("test_pipeline")

def save_local_output(data, filename):
    headers = [
        "campaign_name", "date", "total_spend", "total_clicks", "total_link_clicks", 
        "total_landing_page_views", "total_revenue", "total_purchases", "total_atc", 
        "ctr", "cpc", "cpm", "roas", "click_to_view_ratio", "cpt", "unique_key"
    ]
    
    file_exists = os.path.isfile(filename)
    existing_map = {}
    
    # Read existing
    if file_exists:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for idx, r in enumerate(reader):
                if len(r) > 0:
                    existing_map[r[-1]] = (idx, r)
                    
    inserted = 0
    updated = 0
    
    # Process updates vs inserts (just storing them via dictionary logic to overwrite file)
    final_rows = {}
    if file_exists:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for r in reader:
                if r: final_rows[r[-1]] = r
                
    for row in data:
        key = row[-1]
        if key in final_rows:
            updated += 1
        else:
            inserted += 1
        final_rows[key] = row
        
    # Write fully updated file back simulating upsert to sheet
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _, r in final_rows.items():
            writer.writerow(r)
            
    test_logger.info(f"Local Save -> Upsert Update: {updated} | Insert: {inserted}")
    return inserted, updated

def test_pipeline():
    test_logger.info(f"Starting TEST_PIPELINE. Local Mock Mode with {TEST_HOURS} hours window.")
    stats = {'fetched': 0, 'inserted': 0, 'updated': 0}
    
    try:
        meta_client = MetaAPIClient()
    except Exception as e:
        test_logger.error(f"Failed to intialize Meta Client: {e}")
        return
        
    test_logger.info("=== INITIATING RUN: Fetching Raw Hourly Rows ===")
    raw_data = meta_client.fetch_insights_last_n_hours(hours=TEST_HOURS)
    stats['fetched'] = len(raw_data)
    
    if stats['fetched'] > 0:
        test_logger.info("--- RAW API RESPONSE PURE PARSE (First Record) ---")
        pprint(dict(raw_data[0]))
        test_logger.info("--------------------------------------------------")
        
        test_logger.info("=== TRANFORMING INTO AGGREGATED DAILY METRICS ===")
        aggregated_data = transform_data(raw_data)
        
        test_logger.info("--- FULLY TRANSFORMED UPSERT (First Record) ---")
        pprint(aggregated_data[0])
        test_logger.info("-----------------------------------------------")
        
        if TEST_MODE:
            ins, upd = save_local_output(aggregated_data, LOCAL_OUTPUT_FILE)
            stats['inserted'] += ins
            stats['updated'] += upd
            
            test_logger.info("=== RE-RUNNING LOCAL TO VALIDATE ZERO INSERTS (Only Updates) ===")
            ins2, upd2 = save_local_output(aggregated_data, LOCAL_OUTPUT_FILE)
            if ins2 > 0:
                test_logger.error(f"Idempotency Failed: Re-run inserted {ins2} rows unexpectedly.")
            else:
                test_logger.info(f"Idempotency Check Passed -> Run 2 produced {ins2} inserts and {upd2} safe updates.")

    print("\n=========================================")
    print("      FINAL VALIDATION SUMMARY           ")
    print("=========================================")
    print(f"Total Hourly Rows Extracted: {stats['fetched']}")
    print(f"Total Daily Rows Inserted:   {stats['inserted']}")
    print(f"Total Daily Rows Updated:    {stats['updated']}")
    print("=========================================\n")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    test_pipeline()
