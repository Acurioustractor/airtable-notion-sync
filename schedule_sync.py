import schedule
import time
import subprocess
import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Set up logging
log_file = 'sync_service.log'
logging.basicConfig(
    handlers=[RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)],
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def run_sync():
    logging.info("Starting sync")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, 'sync_airtable_to_notion.py')
    
    try:
        result = subprocess.run(['python3', script_path], capture_output=True, text=True)
        logging.info("Sync output: " + result.stdout)
        if result.stderr:
            logging.error("Errors: " + result.stderr)
    except Exception as e:
        logging.error(f"Error running sync: {str(e)}")
    
    logging.info("Sync completed")

# Schedule the job
schedule.every().day.at("00:00").do(run_sync)

logging.info("Scheduler service started. Will run sync daily at midnight.")

# Run as a service
while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Service stopped by user")
        break
    except Exception as e:
        logging.error(f"Service error: {str(e)}")
        time.sleep(60)  # Wait a bit before retrying 