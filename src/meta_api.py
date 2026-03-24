import os
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

from src.utils import setup_logger, retry, parse_float

logger = setup_logger("meta_api")

class MetaAPIClient:
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        self.ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        self.api_version = os.getenv("META_API_VERSION", "v17.0") # Use fallback if not provided
        
        if not self.access_token or not self.ad_account_id:
            raise ValueError("META_ACCESS_TOKEN and META_AD_ACCOUNT_ID must be set.")
            
        if not self.ad_account_id.startswith("act_"):
            self.ad_account_id = f"act_{self.ad_account_id}"
            
        FacebookAdsApi.init(access_token=self.access_token, api_version=self.api_version)
        self.account = AdAccount(self.ad_account_id)

    def extract_action_value(self, actions_list, action_type, default=0.0):
        if not actions_list:
            return default
        for action in actions_list:
            if action.get("action_type") == action_type:
                return parse_float(action.get("value", 0))
        return default

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def fetch_insights_last_n_hours(self, hours=3):
        debug_mode = os.getenv("DEBUG_MODE", "False").lower() == "true"
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        
        # Ensure dates are always dynamic (Yesterday to Today)
        since_date = yesterday.strftime('%Y-%m-%d')
        until_date = now.strftime('%Y-%m-%d')
        
        params = {
            'level': 'campaign',
            'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
            'time_range': {
                'since': since_date,
                'until': until_date
            },
            'fields': [
                'campaign_id',
                'campaign_name',
                'date_stop',
                'spend',
                'impressions',
                'clicks',
                'actions',
                'action_values',
            ],
            'limit': 2000
        }
        
        logger.info(f"System Time: {now.strftime('%Y-%m-%d %H:%M:%S')} | Fetching: {since_date} to {until_date}")
        insights = self.account.get_insights(params=params)
        insights_list = list(insights) if insights else []
        fetched_count = len(insights_list)
        logger.info(f"RAW DATA: Total records fetched from Meta API: {fetched_count}")
        
        if fetched_count > 0:
            sample_size = min(2, fetched_count)
            logger.info(f"RAW SAMPLE (first {sample_size}): {insights_list[:sample_size]}")

        current_hour = now.hour
        processed_data = []
        all_parsed_records = [] # For fallback
        
        dropped_zero_reach = 0
        dropped_missing_date = 0
        dropped_time_window = 0
        
        for item in insights_list:
            raw_hour_str = item.get('hourly_stats_aggregated_by_advertiser_time_zone')
            date_stop_str = item.get('date_stop') # Switch to date_stop for reporting
            
            if not date_stop_str:
                dropped_missing_date += 1
                continue
                
            spend_val = parse_float(item.get('spend', 0))
            impressions_val = parse_float(item.get('impressions', 0))
            clicks_val = parse_float(item.get('clicks', 0))
            
            # 1. Parse row data for potential use
            start_hour_str = raw_hour_str.split(' - ')[0] if raw_hour_str else "00:00:00"
            actions = item.get('actions', [])
            action_values = item.get('action_values', [])
            
            link_clicks = self.extract_action_value(actions, 'link_click')
            landing_page_views = self.extract_action_value(actions, 'landing_page_view')
            atc = self.extract_action_value(actions, 'add_to_cart')
            purchases = self.extract_action_value(actions, 'purchase')
            revenue = self.extract_action_value(action_values, 'purchase_conversion_value')
            if revenue == 0.0:
                revenue = self.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')

            row_record = {
                'campaign_id': item.get('campaign_id', 'Unknown'),
                'campaign_name': item.get('campaign_name', 'Unknown'),
                'date_stop': date_stop_str,
                'hour': start_hour_str,
                'spend': spend_val,
                'impressions': impressions_val,
                'clicks': clicks_val,
                'link_clicks': link_clicks,
                'landing_page_views': landing_page_views,
                'revenue': revenue,
                'atc': atc,
                'purchases': purchases,
            }
            all_parsed_records.append(row_record)

            # 2. Relaxed Filtering: Keep if impressions > 0 OR spend > 0
            if not debug_mode and (spend_val == 0.0 and impressions_val == 0.0):
                dropped_zero_reach += 1
                continue
                
            # 3. Hourly Window Logic
            try:
                row_hour = int(start_hour_str.split(':')[0])
                # Calculate diff handling wrap-around (e.g., 23 -> 01)
                diff = (current_hour - row_hour) % 24
                
                decision = "KEPT"
                if not debug_mode and diff > hours:
                    decision = "DROPPED (Time Window)"
                    dropped_time_window += 1
                    logger.info(f"Row Hour: {row_hour:02d} | System Hour: {current_hour:02d} | DIFF: {diff}h | {decision}")
                    continue
                
                logger.info(f"Row Hour: {row_hour:02d} | System Hour: {current_hour:02d} | DIFF: {diff}h | {decision}")
                processed_data.append(row_record)

            except (ValueError, IndexError):
                logger.warning(f"Could not parse hour from '{start_hour_str}'. Keeping row as safety fallback.")
                processed_data.append(row_record)
            
        # SAFE FALLBACK: If filtering result in 0 records but we have raw data, use all parsed records
        if len(processed_data) == 0 and len(all_parsed_records) > 0:
            logger.warning("FILTERING FALLBACK: 0 records remained after filtering. Using all available raw records as safety.")
            processed_data = all_parsed_records

        logger.info(f"Summary: Dropped {dropped_zero_reach} zero-reach, {dropped_time_window} outside window.")
        logger.info(f"Final Count: {len(processed_data)} records {'(DEBUG MODE ON)' if debug_mode else ''}.")
        return processed_data

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def fetch_insights_daily_sync(self, days=2):
        debug_mode = os.getenv("DEBUG_MODE", "False").lower() == "true"
        now = datetime.now()
        start_date = now - timedelta(days=days)
        
        params = {
            'level': 'campaign',
            'time_increment': 1,
            'time_range': {
                'since': start_date.strftime('%Y-%m-%d'),
                'until': now.strftime('%Y-%m-%d')
            },
            'fields': [
                'campaign_id',
                'campaign_name',
                'date_stop',
                'spend',
                'impressions',
                'clicks',
                'actions',
                'action_values',
            ],
            'limit': 2000
        }
        
        logger.info(f"Fetching Meta FINAL DAILY SYNC insights from {params['time_range']['since']} to {params['time_range']['until']}")
        insights = self.account.get_insights(params=params)
        insights_list = list(insights) if insights else []
        fetched_count = len(insights_list)
        logger.info(f"RAW DATA: Total daily records fetched from Meta API: {fetched_count}")
        
        if fetched_count > 0:
            sample_size = min(3, fetched_count)
            logger.info(f"RAW DAILY SAMPLE (first {sample_size} records):")
            for i in range(sample_size):
                logger.info(f"Record {i+1}: {insights_list[i]}")

        processed_data = []
        dropped_zero_reach = 0
        dropped_missing_date = 0
        
        for item in insights_list:
            date_stop_str = item.get('date_stop') # Switch to date_stop
            campaign_name = item.get('campaign_name', 'Unknown')
            campaign_id = item.get('campaign_id', 'Unknown')
            
            if not date_stop_str:
                dropped_missing_date += 1
                if not debug_mode: continue
                
            spend_val = parse_float(item.get('spend', 0))
            impressions_val = parse_float(item.get('impressions', 0))
            clicks_val = parse_float(item.get('clicks', 0))
            
            # Relaxed Filtering: Keep if impressions > 0 OR spend > 0
            if not debug_mode and (spend_val == 0.0 and impressions_val == 0.0):
                dropped_zero_reach += 1
                continue
                
            actions = item.get('actions', [])
            action_values = item.get('action_values', [])
            
            link_clicks = self.extract_action_value(actions, 'link_click')
            landing_page_views = self.extract_action_value(actions, 'landing_page_view')
            atc = self.extract_action_value(actions, 'add_to_cart')
            purchases = self.extract_action_value(actions, 'purchase')
            
            revenue = self.extract_action_value(action_values, 'purchase_conversion_value')
            if revenue == 0.0:
                revenue = self.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
            if revenue == 0.0:
                revenue = self.extract_action_value(action_values, 'purchase')

            processed_data.append({
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'date_stop': date_stop_str, # Changed from date_start to date_stop
                'hour': 'SYNC',
                'spend': spend_val,
                'impressions': impressions_val,
                'clicks': clicks_val,
                'link_clicks': link_clicks,
                'landing_page_views': landing_page_views,
                'revenue': revenue,
                'atc': atc,
                'purchases': purchases,
            })
            
        if dropped_zero_reach > 0: logger.info(f"Rows dropped due to zero reach: {dropped_zero_reach}")
        if dropped_missing_date > 0: logger.info(f"Rows dropped due to missing date: {dropped_missing_date}")
        
        logger.info(f"Retrieved and filtered {len(processed_data)} valid daily records {'(DEBUG MODE ON)' if debug_mode else ''}.")
        return processed_data

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def fetch_insights_for_range(self, start_date, end_date):
        """
        Fetches insights for a specific date range.
        Format: YYYY-MM-DD
        """
        params = {
            'level': 'campaign',
            'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
            'time_range': {
                'since': start_date,
                'until': end_date
            },
            'fields': [
                'campaign_id',
                'campaign_name',
                'date_stop',
                'spend',
                'impressions',
                'clicks',
                'actions',
                'action_values',
            ],
            'limit': 2000
        }
        
        logger.info(f"Fetching Meta insights from {start_date} to {end_date}")
        insights = self.account.get_insights(params=params)
        
        processed_data = []
        for item in insights:
            raw_hour = item.get('hourly_stats_aggregated_by_advertiser_time_zone')
            date_start_str = item.get('date_start')
            campaign_name = item.get('campaign_name', 'Unknown')
            campaign_id = item.get('campaign_id', 'Unknown')
            
            if not date_start_str:
                continue
                
            start_hour_str = raw_hour.split(' - ')[0]
            spend_val = parse_float(item.get('spend'))
            impressions_val = parse_float(item.get('impressions'))
            
            if spend_val == 0.0 and impressions_val == 0.0:
                continue
                
            actions = item.get('actions', [])
            action_values = item.get('action_values', [])
            
            link_clicks = self.extract_action_value(actions, 'link_click')
            landing_page_views = self.extract_action_value(actions, 'landing_page_view')
            atc = self.extract_action_value(actions, 'add_to_cart')
            purchases = self.extract_action_value(actions, 'purchase')
            
            revenue = self.extract_action_value(action_values, 'purchase_conversion_value')
            if revenue == 0.0:
                revenue = self.extract_action_value(action_values, 'offsite_conversion.fb_pixel_purchase')
            if revenue == 0.0:
                revenue = self.extract_action_value(action_values, 'purchase')

            processed_data.append({
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'date_start': date_start_str,
                'hour': start_hour_str,
                'spend': spend_val,
                'impressions': impressions_val,
                'clicks': parse_float(item.get('clicks')),
                'link_clicks': link_clicks,
                'landing_page_views': landing_page_views,
                'revenue': revenue,
                'atc': atc,
                'purchases': purchases,
            })
            
        logger.info(f"Retrieved {len(processed_data)} records for specified range.")
        return processed_data

