import asyncio
import os
from datetime import datetime, timedelta, timezone
from utils import now,get_events_hardcoded
from weather_sensor_reading import latest_sensor_reading
import utils
from weather_info  import kalshi_site2nws_site
import order_placer

global site2days
global site_day2mkts

site2days = {}
site_day2mkts = {}
site_day2lsr = {} #lsr = latest sensor reading
site_day2forecast = {} #nws forecast for all available days
site_day2bid = {}
site_day2ask = {}
exchange_status = ""

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
        for kalshi_site in site2days:
            # Format sensor reading with compact timedelta
            nws_site = kalshi_site2nws_site[kalshi_site]
            lsr_publish_time,lsr_value = site_day2lsr.get(kalshi_site)
            lsr_age = now(nws_site) - lsr_publish_time
            lsr_str = f"{lsr_value or 'N/A'} ({format_timedelta_compact(lsr_age)})"
            print(f"{kalshi_site.ljust(5)} LSR: {lsr_str}")
            
            for day in site2days[kalshi_site]:
                print(f"  {day}")
                for mkt in site_day2mkts.get((kalshi_site, day), []):
                    print(f"    {mkt}")
        
        await asyncio.sleep(1)

async def update_positions():
    global mkts2positions
    while True:
        if exchange_status == "closed":
            asyncio.sleep(60)
        for site,days in site_day2mkts.keys():


async def get_markets():
    global site2days, site_day2mkts
    while True:
        site2days = get_events_hardcoded()
        for site in site2days.keys():
            for day in site2days[site]:
                kalshi_mkts = utils.get_markets(day,site)
                mkts = [i for i in kalshi_mkts]
                site_day2mkts[(site,day)] = mkts
        await asyncio.sleep(3600)

async def get_lsr():
    global site_day2mkts,site2lsr
    while True:
        for site in site2days.keys():
            nws_site = kalshi_site2nws_site[site]
            site_day2lsr[site] = latest_sensor_reading(nws_site)
        if exchange_status == "open":
            await asyncio.sleep(3)
        else:
            await asyncio.sleep(60)

async def main():
    try:
        tasks = [
            asyncio.create_task(get_markets()),
            asyncio.create_task(get_lsr()),
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