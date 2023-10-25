from etls.cryptos import StoneCryptoJob
from datetime import datetime
import os 
from logger import logger


current_date = os.getenv('PROCESSING_TIMESTAMP', datetime.now())

if not isinstance(current_date, datetime):
    current_date = datetime.fromisoformat(current_date)
    
logger.info(f"Processando data {current_date}");

job = StoneCryptoJob()
job.execute(date=current_date)

