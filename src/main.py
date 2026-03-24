import sys
from dotenv import load_dotenv

if __name__ == "__main__":
    # Load environment variables for local testing
    load_dotenv()
    
    from src.utils import setup_logger
    logger = setup_logger("main")
    
    try:
        from src.pipeline import run_pipeline
        run_pipeline()
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed", exc_info=True)
        sys.exit(1)
