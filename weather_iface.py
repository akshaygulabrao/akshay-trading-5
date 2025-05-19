import asyncio
import os
from datetime import datetime, timedelta, timezone
from utils import now,get_events_hardcoded
from weather_sensor_reading import latest_sensor_reading
from weather_extract_forecast import forecast_day
import utils
from weather_info import kalshi_site2nws_site,kalshi_sites
import random
import order_placer
from loguru import logger

logger.remove()
logger.add("app.log",level="DEBUG")

global site2days
global client
global site_day2mkts
site2days = {}
kalshi_site_day2mkts = {}
site_day_mkt2price = {}
kalshi_site_day2lsr = {} #lsr = latest sensor reading
kalshi_site2day_forecasts = {} #nws forecast for all available days
site_day2bid = {}
site_day2ask = {}
k_site2efp = {}
kalshi_site2sensor_forecast_diff = {}
exchange_status = ""
client = utils.setup_client()

def format_timedelta_compact(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if seconds or not parts: parts.append(f"{seconds}s")  # Always show at least seconds
    return " ".join(parts)

def get_exchange_status(current_time: datetime) -> tuple:
    """Returns exchange status and timing information"""
    # Base case assumes normal same-day schedule
    exchange_open = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    exchange_close = exchange_open + timedelta(hours=19)  # 8AM + 19h = 3AM next day

    # Adjust for overnight closure period (3AM-8AM)
    if current_time.hour < 3:  # Changed from 8 to 3
        # Still in previous trading day's window
        exchange_open -= timedelta(days=1)
        exchange_close = exchange_open + timedelta(hours=19)
    elif 3 <= current_time.hour < 8:
        # Special closure period, next open is today's 8AM
        exchange_close = exchange_open + timedelta(hours=19)

    # Determine current status
    if 3 <= current_time.hour < 8:
        time_remaining = max(exchange_open - current_time, timedelta(0))
        return ("closed", exchange_open, time_remaining)
    
    time_remaining = max(exchange_close - current_time, timedelta(0))
    return ("open" if current_time < exchange_close else "closed", 
            exchange_close, time_remaining)

async def ui():
    global exchange_status
    """Main user interface loop"""
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        current_time = now("KNYC")
        
        # Get exchange status using helper function
        status, next_event, time_remaining = get_exchange_status(current_time)
        exchange_status = status
        action = "close" if status == "open" else "open"
        
        # Format header using compact timedelta
        print(f"Current Time (KNYC): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Exchange status: {status.capitalize()}")
        print(f"Time until {action}: {format_timedelta_compact(time_remaining)}\n")
        
        # Display market data
        for kalshi_site in kalshi_sites:
            # Format sensor reading with compact timedelta
            nws_site = kalshi_site2nws_site[kalshi_site]
            lsr_publish_time,lsr_value = kalshi_site_day2lsr.get(kalshi_site)
            lsr_age = now(nws_site) - lsr_publish_time
            lsr_err = kalshi_site2sensor_forecast_diff[kalshi_site]
            lsr_str = f"{lsr_value or 'N/A'} ({format_timedelta_compact(lsr_age)})"
            print(f"{kalshi_site.ljust(5)} S:{lsr_str}, SE: {lsr_err:.02f}", end= " forecasts:")
            assert kalshi_site in kalshi_site2day_forecasts
            for day in kalshi_site2day_forecasts[kalshi_site]:
                print(day,end=" ")
            print()
            for day in site2days[kalshi_site]:
                print(f"  {day}")
                for mkt in kalshi_site_day2mkts.get((kalshi_site, day), []):
                    print(f"    {mkt.ljust(25)} {site_day_mkt2price[(kalshi_site,day,mkt)]:.02f}")
        
        await asyncio.sleep(1)

async def update_positions():
    global mkts2positions
    while True:
        if exchange_status == "closed":
            await asyncio.sleep(60)
        else:
            await asyncio.sleep(1)
        for site,days in kalshi_site_day2mkts.keys():
            positions = order_placer.get_positions(client)

async def get_markets():
    global site2days, kalshi_site_day2mkts
    while True:
        site2days = get_events_hardcoded()
        for site in site2days.keys():
            for day in site2days[site]:
                kalshi_mkts = utils.get_markets(day,site)
                mkts = [i for i in kalshi_mkts]
                kalshi_site_day2mkts[(site,day)] = mkts
        await asyncio.sleep(60)

async def get_lsr():
    global kalshi_site_day2mkts,site2lsr
    while True:
        for site in kalshi_sites:
            nws_site = kalshi_site2nws_site[site]
            (d,t),(md,mt)= latest_sensor_reading(nws_site)
            kalshi_site_day2lsr[site] = (d,t)
            latest_sensor_temp = t
            kalshi_site2sensor_forecast_diff[site] = latest_sensor_temp - k_site2efp[site]
            n = len(kalshi_site_day2mkts[(site,site2days[site][0])])
            # for mkt in range(n):
            #     mkt_idx = kalshi_site_day2mkts[(site,site2days[site][0])][mkt]
            #     mkt_strike = utils.extract_num_from_mkt(mkt_idx,site)
            #     if mkt_idx == 0:
            #         if round(mt) > mkt_idx:
            #             site_day_mkt2price[(site,)]

        if exchange_status == "open":
            await asyncio.sleep(1)
        else:
            await asyncio.sleep(60)

async def akshay_price():
    global site_day_mkt2price
    while True:
        for kalshi_site in kalshi_sites:
            for day in site2days[kalshi_site]:
                for mkt in range(len(kalshi_site_day2mkts[(kalshi_site,day)])):
                    mkt_name = kalshi_site_day2mkts[(kalshi_site,day)][mkt]
                    logger.debug(f"{kalshi_site}{day}{mkt_name}")
                    site_day_mkt2price[(kalshi_site,day,mkt_name)] = random.random()
        await asyncio.sleep(10)

async def get_forecast():
    global kalshi_site2day_forecasts,kalshi_site2sensor_forecast_diff,k_site2efp
    while True:
        for site in kalshi_sites:
            nws_site = kalshi_site2nws_site[site]
            forecasts,earliest_forecast_pred = forecast_day(nws_site)
            kalshi_site2day_forecasts[site] = forecasts[:2]
            k_site2efp[site]= float(earliest_forecast_pred)

        await asyncio.sleep(10)

async def main():
    try:
        tasks = [
            asyncio.create_task(get_markets()),
            asyncio.create_task(get_forecast()),
            asyncio.create_task(get_lsr()),
            asyncio.create_task(akshay_price()),
            asyncio.create_task(ui())
        ]
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\nStopped time display.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")