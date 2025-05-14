import asyncio
from datetime import datetime,timedelta
import os
from pathlib import Path

import pandas as pd
from loguru import logger

from weather_extract_forecast import extract_forecast
from weather_info import sites2city
from utils import now



async def main():
    logger.remove()
    logger.add('forecast_logs/ob.log',
               rotation="24 hours", 
               retention="3 days",
               enqueue=True)
    while True:
        current_date = datetime.now().strftime('%Y-%m-%d')
        for site in sites2city.keys():
            output_dir = Path(f"forecasts/{site}/{current_date}/")
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%H%M')
            output_path = output_dir / f"forecast_{timestamp}.csv"
            logger.info(f'{site}')

            try:
                df = extract_forecast(site)
            except Exception:
                logger.error(f"Site {site} is down")
                print("site is down", file=open(output_path,"w+",encoding="utf-8"))
                continue

            df.to_csv(output_path)
            logger.info(f'Saved forecast to {output_path}')
        
        n = now()
        next_hour = (n.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        time_left = next_hour - n
        await asyncio.sleep(time_left.total_seconds())

if __name__ == "__main__":
    asyncio.run(main())