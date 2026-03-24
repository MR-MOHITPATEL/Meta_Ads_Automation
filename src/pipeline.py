import os
from datetime import datetime
from src.utils import setup_logger, safe_divide, get_current_ist_time


logger = setup_logger("pipeline")

def get_week_of_month(dt):
    """
    Logic:
    Days 1–7 → 1st week
    Days 8–14 → 2nd week
    Days 15–21 → 3rd week
    Days 22–28 → 4th week
    Days 29–31 → 5th week
    """
    day = dt.day
    if 1 <= day <= 7: week = "1st"
    elif 8 <= day <= 14: week = "2nd"
    elif 15 <= day <= 21: week = "3rd"
    elif 22 <= day <= 28: week = "4th"
    else: week = "5th"
    return f"{week} week of {dt.strftime('%B')}"

def format_date_custom(dt_str):
    """Formats YYYY-MM-DD to DD-Month-YYYY"""
    dt = datetime.strptime(dt_str, '%Y-%m-%d')
    return dt.strftime('%d-%B-%Y')

def validate_data(data):
    """
    Validates data before pushing to Sheets.
    Checks for nulls in key fields (Name and Date).
    """
    seen_keys = set()
    for i, row in enumerate(data):
        # Key is now (Name, Date) at index 0 and 1
        key = f"{row[0]}_{row[1]}"
        seen_keys.add(key)
        
        if not row[0] or not row[1]:
            raise ValueError(f"VALIDATION FAILED: Null value in key fields at row {i}")
            
    return True

def transform_data(raw_data):
    """
    Aggregates records into DAILY rows per campaign.
    Calculates derived metrics over the aggregated totals.
    """
    logger.info(f"Step: Transformation -> Received {len(raw_data)} raw records from API")
    extraction_hour = get_current_ist_time().strftime("%H:00")
    
    aggregated = {}
    
    for row in raw_data:
        # Internal aggregation still uses campaign_id for robustness if available
        # But we key by (Name, Date) if we want to match the display-only logic perfectly later
        # Actually, let's stick to campaign_id for internal aggregation to be safe.
        id_key = f"{row['campaign_id']}_{row['date_stop']}"
        
        if id_key not in aggregated:
            aggregated[id_key] = {
                'campaign_id': row['campaign_id'],
                'campaign_name': row['campaign_name'],
                'date_stop': row['date_stop'],
                'total_spend': 0.0,
                'total_impressions': 0.0,
                'total_clicks': 0.0,
                'total_link_clicks': 0.0,
                'total_landing_page_views': 0.0,
                'total_revenue': 0.0,
                'total_purchases': 0.0,
                'total_atc': 0.0,
                'latest_hour': '00:00:00'
            }
            
        agg = aggregated[id_key]
        agg['total_spend'] += row['spend']
        agg['total_impressions'] += row.get('impressions', 0.0)
        agg['total_clicks'] += row['clicks']
        agg['total_link_clicks'] += row['link_clicks']
        agg['total_landing_page_views'] += row['landing_page_views']
        agg['total_revenue'] += row['revenue']
        agg['total_purchases'] += row['purchases']
        agg['total_atc'] += row['atc']
        # Keep the most recent hour for freshness reporting
        if row['hour'] > agg['latest_hour']:
            agg['latest_hour'] = row['hour']

    # Final result list
    transformed = []
    
    # OUTPUT SCHEMA (Display Only)
    schemas = [
        "campaign_name", "date", "week_of_month", "spend", "cpm", "cpc", "ctr", 
        "link_clicks", "web_page_views", "click_to_view_ratio", "cpt", "revenue", "roas", "atc",
        "impressions", "data_hour", "pipeline_run_time", "freshness_lag_hours"
    ]
    
    # Sort by date ascending (date_stop)
    sorted_items = sorted(aggregated.items(), key=lambda x: x[1]['date_stop'])
    
    run_time = get_current_ist_time()
    current_hour = run_time.hour

    for id_key, agg in sorted_items:
        dt = datetime.strptime(agg['date_stop'], '%Y-%m-%d')
        formatted_date = dt.strftime('%d-%B-%Y')
        week_str = get_week_of_month(dt)
        
        # Calculate Freshness Metrics
        try:
            # hour is "14:00:00" -> 14
            latest_h = str(agg.get('latest_hour', '00:00:00'))
            data_hour_int = int(latest_h.split(':')[0]) if ':' in latest_h else 0
            lag = (current_hour - data_hour_int) % 24
        except (ValueError, AttributeError, IndexError):
            data_hour_int = 0
            lag = 0

        # Local variables with explicit types for calculations
        spend = float(agg['total_spend'])
        impressions = int(agg['total_impressions'])
        clicks = float(agg['total_clicks'])
        link_clicks = int(agg['total_link_clicks'])
        lpage_views = int(agg['total_landing_page_views'])
        revenue = float(agg['total_revenue'])
        purchases = float(agg['total_purchases'])
        atc = int(agg['total_atc'])

        row_dict = {
            "campaign_name": str(agg['campaign_name']),
            "date": formatted_date,
            "week_of_month": week_str,
            "spend": round(spend, 2),
            "impressions": impressions,
            "link_clicks": link_clicks,
            "web_page_views": lpage_views,
            "revenue": round(revenue, 2),
            "atc": atc,
            "data_hour": data_hour_int,
            "pipeline_run_time": run_time.strftime('%d-%m-%Y %H:%M'),
            "freshness_lag_hours": lag
        }
        
        # Extended calculations
        row_dict["cpm"] = round((spend / impressions * 1000), 2) if impressions > 0 else 0.0
        row_dict["cpc"] = round((spend / clicks), 2) if clicks > 0 else 0.0
        row_dict["ctr"] = f"{round((clicks / impressions * 100), 2)}%" if impressions > 0 else "0.00%"
        row_dict["click_to_view_ratio"] = f"{round((float(lpage_views) / float(link_clicks) * 100), 2)}%" if link_clicks > 0 else "0.00%"
        row_dict["cpt"] = round((spend / purchases), 2) if purchases > 0 else 0.0
        row_dict["roas"] = round((revenue / spend), 2) if spend > 0 else 0.0
        
        # Create ordered row based on schemas
        ordered_row = [row_dict.get(col, 0) for col in schemas]
        transformed.append(ordered_row)

    validate_data(transformed)
    logger.info(f"Aggregated records into {len(transformed)} daily display rows.")
    return transformed

def run_hourly_pipeline(test_mode=False, start_date=None, end_date=None, hours=3):
    from src.meta_api import MetaAPIClient
    from src.sheets import GoogleSheetsClient
    
    logger.info(f"=== Starting {'TEST ' if test_mode else 'DAILY '}Aggregation Pipeline ===")
    
    try:
        meta_client = MetaAPIClient()
        sheets_client = GoogleSheetsClient(test_mode=test_mode)
        
        if start_date and end_date:
            raw_insights = meta_client.fetch_insights_for_range(start_date, end_date)
        else:
            raw_insights = meta_client.fetch_insights_last_n_hours(hours=hours)
        
        if not raw_insights:
            logger.info("No data fetched. Pipeline complete.")
            return
            
        transformed_data = transform_data(raw_insights)
        inserted_count, updated_count = sheets_client.upsert_rows(transformed_data)
        
        logger.info(f"=== Pipeline Completed. Inserts: {inserted_count} | Updates: {updated_count} ===")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

def run_daily_sync_pipeline():
    run_hourly_pipeline(hours=48) # Simplified to use same aggregation logic

def run_pipeline():
    mode = os.getenv("PIPELINE_MODE", "HOURLY").upper()
    if mode == "DAILY":
        run_daily_sync_pipeline()
    else:
        run_hourly_pipeline()


