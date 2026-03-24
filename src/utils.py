import logging
import time
from functools import wraps

def setup_logger(name="meta_pipeline"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.
    Also natively guards against Meta API rate limits by enforcing a longer pause.
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    # Detect Meta API rate limits (e.g., Code 17)
                    is_rate_limit = False
                    pause_time = mdelay
                    
                    if "FacebookRequestError" in str(type(e)):
                        # Crude but definitive rate-limit detection avoiding circular imports
                        if "User request limit reached" in str(e) or "17" in str(e):
                            is_rate_limit = True
                            pause_time = 120 # Pause for 2 full minutes on rate limits
                            
                    if is_rate_limit:
                        msg = f"Meta Rate Limit Hit! Pausing execution for {pause_time}s... (Attempts left: {mtries-1})"
                    else:
                        msg = f"Exception: {str(e)[:100]}, Retrying in {pause_time} seconds... (Attempts left: {mtries-1})"
                        
                    if logger:
                        logger.warning(msg)
                        
                    time.sleep(pause_time)
                    mtries -= 1
                    
                    # Do not compound the backoff multiplier continuously if it was a rate limit
                    if not is_rate_limit:
                        mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

def safe_divide(numerator, denominator, default_value=0.0):
    """
    Safe division to prevent ZeroDivisionError and handle invalid types.
    """
    try:
        num = float(numerator) if numerator is not None else 0.0
        den = float(denominator) if denominator is not None else 0.0
        if den == 0.0:
            return default_value
        return num / den
    except (ValueError, TypeError):
        return default_value

def parse_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default
