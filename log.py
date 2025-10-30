import os, logging

LOG_FILE = 'etl_process.log'

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'

logging.basicConfig(
    level=logging.INFO, 
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),     
        logging.StreamHandler()           
    ]
)

logger = logging.getLogger('ETL-main')