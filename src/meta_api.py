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
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        
        params = {
            'level': 'campaign',
            'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
            'time_range': {
                'since': yesterday.strftime('%Y-%m-%d'),
                'until': now.strftime('%Y-%m-%d')
            },
            'fields': [
                'campaign_id',
                'campaign_name',
                'date_start',
                'spend',
                'impressions',
                'clicks',
                'actions',
                'action_values',
            ],
            'limit': 2000
        }
        
        logger.info(f"Fetching Meta insights from {params['time_range']['since']} to {params['time_range']['until']}")
        insights = self.account.get_insights(params=params)
        insights_list = list(insights) if insights else []
        fetched_count = len(insights_list)
        logger.info(f"DEBUG VALIDATION: Total hours fetched from Meta API: {fetched_count}")
        
        cutoff_time = now - timedelta(hours=hours)
        processed_data = []
        
        for item in insights_list:
            raw_hour = item.get('hourly_stats_aggregated_by_advertiser_time_zone')
            date_start_str = item.get('date_start')
            campaign_name = item.get('campaign_name', 'Unknown')
            campaign_id = item.get('campaign_id', 'Unknown')
            
            if not date_start_str:
                continue
                
            start_hour_str = raw_hour.split(' - ')[0]
            
            spend_val = parse_float(item.get('spend'))
            impressions_val = parse_float(item.get('impressions'))
            
            # ISSUE 2 & 3: Drop ONLY if spend and impressions are both zero.
            if spend_val == 0.0 and impressions_val == 0.0:
                continue
                
            try:
                dt_str = f"{date_start_str} {start_hour_str}"
                insight_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                
                # Filter for the last `hours` window
                if insight_dt < cutoff_time:
                    continue
                        
            except ValueError:
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
                'spend': parse_float(item.get('spend')),
                'impressions': parse_float(item.get('impressions')),
                'clicks': parse_float(item.get('clicks')),
                'link_clicks': link_clicks,
                'landing_page_views': landing_page_views,
                'revenue': revenue,
                'atc': atc,
                'purchases': purchases,
            })
            
        logger.info(f"Retrieved and filtered {len(processed_data)} valid records.")
        return processed_data

    @retry(Exception, tries=5, delay=5, backoff=2, logger=logger)
    def fetch_insights_daily_sync(self, days=2):
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
                'campaign_name',
                'date_start',
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
        logger.info(f"DEBUG VALIDATION: Total daily synced rows fetched from Meta API: {fetched_count}")
        
        processed_data = []
        
        for item in insights_list:
            date_start_str = item.get('date_start')
            campaign_name = item.get('campaign_name', 'Unknown')
            campaign_id = item.get('campaign_id', 'Unknown')
            
            if not date_start_str:
                continue
                
            spend_val = parse_float(item.get('spend'))
            impressions_val = parse_float(item.get('impressions'))
            
            # ISSUE 2 & 3: Drop ONLY if spend and impressions are both zero.
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
                'hour': 'SYNC', # Placeholder hour, pipeline aggregator drops hour regardless
                'spend': spend_val,
                'impressions': impressions_val,
                'clicks': parse_float(item.get('clicks')),
                'link_clicks': link_clicks,
                'landing_page_views': landing_page_views,
                'revenue': revenue,
                'atc': atc,
                'purchases': purchases,
            })
            
        logger.info(f"Retrieved and filtered {len(processed_data)} valid daily finalized records.")
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
                'date_start',
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

