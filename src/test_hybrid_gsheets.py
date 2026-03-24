import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.utils import setup_logger
from src.meta_api import MetaAPIClient
from src.pipeline import transform_data
from src.sheets import GoogleSheetsClient

load_dotenv()

logger = setup_logger("hybrid_test_gsheets")

class HybridTestPipeline:
    def __init__(self):
        self.TEST_MODE = True
        self.meta_client = MetaAPIClient()
        self.sheets_client = GoogleSheetsClient(sheet_name="Hybrid Test Sandbox")
        self.now = datetime.now()
        # Today represented natively
        self.today_str = self.now.strftime('%Y-%m-%d')
        print(f"\n[CONFIGURATION] TARGET_DATE detected as: {self.today_str}")

    def run_safe_test(self):
        print("\n--- STEP 2: HOURLY PIPELINE (REAL-TIME) ---")
        # Hunt backward dynamically for a date with actual active spend history properly validating Overrides
        raw_hourly = []
        target_date_obj = None
        for i in range(0, 30):
            test_date = self.now - timedelta(days=i)
            since_str = test_date.strftime('%Y-%m-%d')
            until_str = test_date.strftime('%Y-%m-%d')
            
            params = {
                'level': 'campaign',
                'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
                'time_range': {'since': since_str, 'until': until_str},
                'fields': ['campaign_name', 'date_start', 'spend', 'impressions', 'clicks', 'actions', 'action_values'],
                'limit': 500
            }
            res = list(self.meta_client.account.get_insights(params=params))
            if len(res) > 0:
                self.today_str = since_str # Mock 'today' natively tracking active history bounds
                
                # Accurately recreate the Pre-Aggregated Parsing Array structure natively
                for item in res:
                    spend_val = float(item.get('spend', 0))
                    imp_val = float(item.get('impressions', 0))
                    if spend_val == 0 and imp_val == 0: continue
                    
                    actions = item.get('actions', [])
                    action_values = item.get('action_values', [])
                    rev = self.meta_client.extract_action_value(action_values, 'purchase_conversion_value')
                    if rev == 0: rev = self.meta_client.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
                    if rev == 0: rev = self.meta_client.extract_action_value(action_values, 'purchase')
                    
                    raw_hourly.append({
                        'campaign_name': item.get('campaign_name'),
                        'date_start': item.get('date_start'),
                        'hour': item.get('hourly_stats_aggregated_by_advertiser_time_zone', '').split(' - ')[0],
                        'spend': spend_val,
                        'impressions': imp_val,
                        'clicks': float(item.get('clicks', 0)),
                        'link_clicks': self.meta_client.extract_action_value(actions, 'link_click'),
                        'landing_page_views': self.meta_client.extract_action_value(actions, 'landing_page_view'),
                        'revenue': rev,
                        'atc': self.meta_client.extract_action_value(actions, 'add_to_cart'),
                        'purchases': self.meta_client.extract_action_value(actions, 'purchase'),
                    })
                    
                target_date_obj = test_date
                print(f"[*] Found active Hourly Aggregation Data on TARGET_DATE: {self.today_str}")
                break
                
        if not target_date_obj:
            print("No active ad data found across last 30 loops.")
            return
            
        hourly_transformed = transform_data(raw_hourly)
        
        # Build dictionary indexing uniquely per campaign_name + date_start
        hourly_dict = {row[-1]: row for row in hourly_transformed}

        print("\n--- STEP 3: DAILY PIPELINE (FINAL DATA) ---")
        # Fetch identical date exactly executing time_increment=1 implicitly mapping Final API overrides
        daily_params = {
            'level': 'campaign',
            'time_increment': 1,
            'time_range': {'since': self.today_str, 'until': self.today_str},
            'fields': ['campaign_name', 'date_start', 'spend', 'impressions', 'clicks', 'actions', 'action_values'],
            'limit': 500
        }
        raw_daily_resp = list(self.meta_client.account.get_insights(params=daily_params))
        raw_daily = []
        for item in raw_daily_resp:
            spend_val = float(item.get('spend', 0))
            imp_val = float(item.get('impressions', 0))
            if spend_val == 0 and imp_val == 0: continue
            
            actions = item.get('actions', [])
            action_values = item.get('action_values', [])
            rev = self.meta_client.extract_action_value(action_values, 'purchase_conversion_value')
            if rev == 0: rev = self.meta_client.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
            if rev == 0: rev = self.meta_client.extract_action_value(action_values, 'purchase')
            
            raw_daily.append({
                'campaign_name': item.get('campaign_name'),
                'date_start': item.get('date_start'),
                'hour': 'SYNC',
                'spend': spend_val,
                'impressions': imp_val,
                'clicks': float(item.get('clicks', 0)),
                'link_clicks': self.meta_client.extract_action_value(actions, 'link_click'),
                'landing_page_views': self.meta_client.extract_action_value(actions, 'landing_page_view'),
                'revenue': rev,
                'atc': self.meta_client.extract_action_value(actions, 'add_to_cart'),
                'purchases': self.meta_client.extract_action_value(actions, 'purchase'),
            })

        daily_transformed = transform_data(raw_daily)
        
        daily_dict = {row[-1]: row for row in daily_transformed}
        
        print("\n--- STEP 4 & 7: OVERRIDE LOGIC & DEBUG LOGGING ---")
        
        final_output_rows = []
        # Merge keys across both pipelines
        all_keys = set(hourly_dict.keys()).union(set(daily_dict.keys()))
        
        existing_sheet_map = self.sheets_client.get_existing_keys_with_index()
        
        for key in all_keys:
            # Reconstruct identifier logic mapping
            parts = key.split('_')
            date_val = parts[-1]
            campaign = "_".join(parts[:-1])
            
            h_row = hourly_dict.get(key)
            d_row = daily_dict.get(key)
            
            h_spend = h_row[3] if h_row else 0.0
            d_spend = d_row[3] if d_row else 0.0
            
            print(f"\nTarget Key: {key}")
            print(f"-> Hourly Aggregated Spend: {h_spend}")
            if d_row:
                print(f"-> Daily API Spend Found:   {d_spend}")
            else:
                print("-> Daily API Spend:         None")
                
            # EXECUTE LOGICAL OVERRIDE RULES
            selected_row = None
            if d_row and date_val < self.today_str:
                print("   [RULE] Date < Today + Daily Data Exists -> OVERWRITING hourly entirely.")
                selected_row = d_row
            elif date_val == self.today_str:
                if h_row:
                    print("   [RULE] Date == Today -> USING HOURLY real-time natively (NO overwrite yet).")
                    selected_row = h_row
                else: 
                    # If only daily exists for today (rare timing) fallback
                    selected_row = d_row
            else:
                # Catch conditions where only one pipeline yielded
                selected_row = h_row if h_row else d_row
                
            final_val = selected_row[2]
            print(f"-> Final Value safely queued for Sheet: {final_val}")
            
            action = "UPDATE" if key in existing_sheet_map else "INSERT"
            print(f"-> Target Sheet Action Queue: {action}")
            
            final_output_rows.append(selected_row)
            
        print("\n--- STEP 5: UPSERT LOGIC TO GOOGLE SHEETS ---")
        if not final_output_rows:
            print("No generated target output data evaluated. Testing completed.")
            return

        inserts, updates = self.sheets_client.upsert_rows(final_output_rows)
        print(f"\n✅ SUCCESSFULLY PROCESSED {len(final_output_rows)} ROWS!")
        print(f"   Inserts Completed: {inserts} | Updates Completed: {updates}\n")

if __name__ == "__main__":
    tester = HybridTestPipeline()
    tester.run_safe_test()
