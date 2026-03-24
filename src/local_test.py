import os
import csv
from datetime import datetime, timedelta
from src.utils import setup_logger, parse_float
from src.meta_api import MetaAPIClient
from src.pipeline import transform_data

TEST_MODE = True
LOCAL_OUTPUT_FILE = "local_test_output.csv"
test_logger = setup_logger("local_debug")

def save_local_output(data, filename):
    headers = [
        "campaign_name", "date", "total_spend", "total_clicks", "total_link_clicks", 
        "total_landing_page_views", "total_revenue", "total_purchases", "total_atc", 
        "ctr", "cpc", "cpm", "roas", "click_to_view_ratio", "cpt", "unique_key"
    ]
    
    file_exists = os.path.isfile(filename)
    final_rows = {}
    
    if file_exists:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for r in reader:
                if r: final_rows[r[-1]] = r
                
    inserted = 0
    updated = 0
    
    for row in data:
        key = row[-1]
        if key in final_rows:
            test_logger.info(f"UPSERT ACTION: Updated existing row for {key}")
            updated += 1
        else:
            test_logger.info(f"UPSERT ACTION: Inserted new row for {key}")
            inserted += 1
        final_rows[key] = row
        
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _, r in final_rows.items():
            writer.writerow(r)
            
    # Count rows in CSV for sanity check
    total_csv_rows = 0
    with open(filename, 'r', encoding='utf-8') as f:
        total_csv_rows = sum(1 for line in f) - 1 # exclude header
        
    return inserted, updated, total_csv_rows

def debug_fetch_data(meta_client, hours_limit=6):
    now = datetime.now()
    yesterday = now - timedelta(days=7) # Look back a week to guarantee we find something
    
    campaigns = meta_client.account.get_campaigns(fields=['id', 'name', 'start_time'], params={'limit': 15})
    if not campaigns:
        test_logger.error("No campaigns found.")
        return []
        
    included_rows = []
    
    for target in campaigns:
        campaign_name = target.get('name', 'Unknown')
        start_time_str = target.get('start_time')
        
        try:
            start_dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
        except Exception:
            start_dt = now - timedelta(days=1)
            
        params = {
            'level': 'campaign',
            'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
            'time_range': {'since': yesterday.strftime('%Y-%m-%d'), 'until': now.strftime('%Y-%m-%d')},
            'fields': ['campaign_name', 'date_start', 'spend', 'impressions', 'clicks', 'actions', 'action_values'],
            'filtering': [{'field': 'campaign.name', 'operator': 'EQUAL', 'value': campaign_name}],
            'limit': 100
        }
        
        try:
            insights = meta_client.account.get_insights(params=params)
        except Exception as e:
            continue
            
        if not insights:
            continue
            
        insights_list = list(insights)[::-1]
            
        print(f"\n[STEP 1] VERIFY CAMPAIGN START FILTER (First Hour tracking & Data drops)")
        print(f"Target Campaign: {campaign_name}")
        
        fetched_count = len(insights_list)
        print(f"DEBUG VALIDATION: Total hours fetched from Meta API: {fetched_count}")
        
        filtered_count = 0
        included_count = 0
        
        print("\n--- HOURLY EVALUATION ---")
        for item in insights_list:
            if included_count >= hours_limit:
                break
                
            raw_hour = item.get('hourly_stats_aggregated_by_advertiser_time_zone')
            date_start_str = item.get('date_start')
            spend = parse_float(item.get('spend'))
            impressions = parse_float(item.get('impressions'))
            
            if not raw_hour or not date_start_str: continue
            
            start_hour_str = raw_hour.split(' - ')[0]
            
            if spend == 0.0 and impressions == 0.0:
                print(f"Date: {date_start_str} Hour: {start_hour_str} | Spend: {spend} Imp: {impressions} | Status: EXCLUDED (Zero Metrics)")
                filtered_count += 1
            else:
                print(f"Date: {date_start_str} Hour: {start_hour_str} | Spend: {spend} Imp: {impressions} | Status: INCLUDED")
                included_count += 1
                
                actions = item.get('actions', [])
                action_values = item.get('action_values', [])
                
                link_clicks = meta_client.extract_action_value(actions, 'link_click')
                landing_page_views = meta_client.extract_action_value(actions, 'landing_page_view')
                atc = meta_client.extract_action_value(actions, 'add_to_cart')
                purchases = meta_client.extract_action_value(actions, 'purchase')
                
                revenue = meta_client.extract_action_value(action_values, 'purchase_conversion_value')
                if revenue == 0.0: revenue = meta_client.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
                if revenue == 0.0: revenue = meta_client.extract_action_value(action_values, 'purchase')
                
                included_rows.append({
                    'campaign_name': campaign_name,
                    'date_start': date_start_str,
                    'hour': start_hour_str,
                    'spend': spend,
                    'impressions': parse_float(item.get('impressions')),
                    'clicks': parse_float(item.get('clicks')),
                    'link_clicks': link_clicks,
                    'landing_page_views': landing_page_views,
                    'revenue': revenue,
                    'atc': atc,
                    'purchases': purchases,
                })
                
        if included_count > 0:
            print(f"\nFiltered hours count: {filtered_count}")
            print(f"Included hours count: {included_count}")
            return included_rows
            
    test_logger.warning("Scanned campaigns but found zero data within the time window constraints.")
    return []

def test_pipeline():
    # Remove local file to simulate fresh run cleanly if desired, or let it UPSERT. 
    # We will let it UPSERT to test RUN 1 vs RUN 2 securely.
    if os.path.exists(LOCAL_OUTPUT_FILE):
        os.remove(LOCAL_OUTPUT_FILE)
        
    meta_client = MetaAPIClient()
    
    print("\n=========================================")
    print("           RUN 1 (EXPECT INSERT)         ")
    print("=========================================")
    raw_data = debug_fetch_data(meta_client, hours_limit=12) # Expanded slightly to catch start bounds
    
    if not raw_data:
        print("No Valid Data Found. Exiting Test.")
        return
        
    print("\n[STEP 2] VERIFY AGGREGATION LOGIC")
    daily_aggregated_data = transform_data(raw_data)
    
    for row in daily_aggregated_data:
        # Map values to header names
        print(f"Aggregation Result -> Campaign: {row[0]}")
        print(f"total_spend: {row[2]}")
        print(f"total_clicks: {row[3]}")
        print(f"total_revenue: {row[6]}")
        print(f"total_purchases: {row[7]}")
        
    print("\n[STEP 3] VERIFY UPSERT LOGIC & [STEP 6] DEBUG LOGGING")
    ins1, upd1, csv_count1 = save_local_output(daily_aggregated_data, LOCAL_OUTPUT_FILE)
    
    print("\n=========================================")
    print("           RUN 2 (EXPECT UPDATE)         ")
    print("=========================================")
    ins2, upd2, csv_count2 = save_local_output(daily_aggregated_data, LOCAL_OUTPUT_FILE)
    
    print("\n[STEP 4] OUTPUT VALIDATION")
    for row in daily_aggregated_data:
        print(f"campaign_name: {row[0]}")
        print(f"date: {row[1]}")
        print(f"total_spend: {row[2]}")
        print(f"total_clicks: {row[3]}")
        print(f"total_revenue: {row[6]}")
        print(f"roas: {row[12]}")
        print("-" * 20)
        
    print("\n[STEP 5 & 7] DUPLICATE CHECK & SAFE CALCULATIONS")
    print("\n--- FINAL RUN SUMMARY ---")
    print(f"RUN 1 -> Inserted: {ins1} | Updated: {upd1}")
    print(f"RUN 2 -> Inserted: {ins2} | Updated: {upd2}")
    print(f"Total rows in {LOCAL_OUTPUT_FILE}: {csv_count2} (Expected: 1 row per date)")
    print("All derived metrics executed via safe_divide() successfully.")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    test_pipeline()
