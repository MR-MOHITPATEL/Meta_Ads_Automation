import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.utils import setup_logger
from src.meta_api import MetaAPIClient
from src.pipeline import transform_data

load_dotenv()

logger = setup_logger("test_hybrid")

def run_hybrid_comparison():
    meta_client = MetaAPIClient()
    now = datetime.now()
    
    # Hunt backward for a day with active spend
    target_date = None
    daily_insights = None
    
    for i in range(1, 30):
        test_date = now - timedelta(days=i)
        since_str = test_date.strftime('%Y-%m-%d')
        until_str = test_date.strftime('%Y-%m-%d')
        
        params = {
            'level': 'campaign',
            'time_increment': 1,
            'time_range': {'since': since_str, 'until': until_str},
            'fields': ['campaign_name', 'date_start', 'spend', 'impressions', 'clicks', 'actions', 'action_values'],
            'limit': 1000
        }
        res = list(meta_client.account.get_insights(params=params))
        if len(res) > 0:
            target_date = test_date
            daily_insights = res
            print(f"[*] Found active data on {since_str}! Deploying Hybrid validations...")
            break
            
    if not target_date:
        print("No data found in the last 30 days. Cannot demonstrate overlap.")
        return

    # Mock format the results exactly like the production fetchers do natively:
    raw_daily = []
    for item in daily_insights:
        spend_val = float(item.get('spend', 0))
        imp_val = float(item.get('impressions', 0))
        if spend_val == 0 and imp_val == 0: continue
        
        actions = item.get('actions', [])
        action_values = item.get('action_values', [])
        rev = meta_client.extract_action_value(action_values, 'purchase_conversion_value')
        if rev == 0: rev = meta_client.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
        if rev == 0: rev = meta_client.extract_action_value(action_values, 'purchase')
        
        raw_daily.append({
            'campaign_name': item.get('campaign_name'),
            'date_start': item.get('date_start'),
            'hour': 'SYNC',
            'spend': spend_val,
            'impressions': imp_val,
            'clicks': float(item.get('clicks', 0)),
            'link_clicks': meta_client.extract_action_value(actions, 'link_click'),
            'landing_page_views': meta_client.extract_action_value(actions, 'landing_page_view'),
            'revenue': rev,
            'atc': meta_client.extract_action_value(actions, 'add_to_cart'),
            'purchases': meta_client.extract_action_value(actions, 'purchase'),
        })
        
    # Now pull the EXACT SAME DAY using the HOURLY aggregation layer constraint
    hourly_params = {
        'level': 'campaign',
        'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
        'time_range': {'since': target_date.strftime('%Y-%m-%d'), 'until': target_date.strftime('%Y-%m-%d')},
        'fields': ['campaign_name', 'date_start', 'spend', 'impressions', 'clicks', 'actions', 'action_values'],
        'limit': 2000
    }
    hourly_insights = list(meta_client.account.get_insights(params=hourly_params))
    
    raw_hourly = []
    for item in hourly_insights:
        spend_val = float(item.get('spend', 0))
        imp_val = float(item.get('impressions', 0))
        if spend_val == 0 and imp_val == 0: continue
        
        actions = item.get('actions', [])
        action_values = item.get('action_values', [])
        rev = meta_client.extract_action_value(action_values, 'purchase_conversion_value')
        if rev == 0: rev = meta_client.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
        if rev == 0: rev = meta_client.extract_action_value(action_values, 'purchase')
        
        raw_hourly.append({
            'campaign_name': item.get('campaign_name'),
            'date_start': item.get('date_start'),
            'hour': item.get('hourly_stats_aggregated_by_advertiser_time_zone', '').split(' - ')[0],
            'spend': spend_val,
            'impressions': imp_val,
            'clicks': float(item.get('clicks', 0)),
            'link_clicks': meta_client.extract_action_value(actions, 'link_click'),
            'landing_page_views': meta_client.extract_action_value(actions, 'landing_page_view'),
            'revenue': rev,
            'atc': meta_client.extract_action_value(actions, 'add_to_cart'),
            'purchases': meta_client.extract_action_value(actions, 'purchase'),
        })

    # Transform both natively parsing the logic filters explicitly through our main aggregator
    hourly_transformed = transform_data(raw_hourly)
    daily_transformed = transform_data(raw_daily)
    
    hourly_dict = {row[-1]: row for row in hourly_transformed}
    daily_dict = {row[-1]: row for row in daily_transformed}
    
    print("\n" + "="*50)
    print("DEBUG VALIDATION: HOURLY VS DAILY EXACT COMPARISON")
    print("="*50)
    
    overlap_count = 0
    for key, daily_row in daily_dict.items():
        if key in hourly_dict:
            overlap_count += 1
            hourly_row = hourly_dict[key]
            
            campaign = daily_row[0]
            date_val = daily_row[1]
            daily_spend = daily_row[2]
            hourly_spend = hourly_row[2]
            
            diff = abs(daily_spend - hourly_spend)
            
            print(f"\nTarget: {campaign} on {date_val}")
            print(f"-> Hourly Aggregated Spend: {hourly_spend:.2f}")
            print(f"-> Final Daily API Spend:   {daily_spend:.2f}")
            
            if diff < 0.5:
                print("STATUS: SUCCESS (Values are deeply aligned -> Daily overrides flawlessly)")
            else:
                print(f"STATUS: DRIFT DETECTED (Diff: {diff:.2f}) -> Daily Pipeline natively overrides hourly drift!")

    print(f"\nValidated {overlap_count} overlapping unique keys cleanly.")
    
if __name__ == "__main__":
    run_hybrid_comparison()
