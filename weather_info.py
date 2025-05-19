nws_sites = {
    "KNYC": "https://www.weather.gov/wrh/timeseries?site=KNYC&hours=168",
    "KMDW": "https://www.weather.gov/wrh/timeseries?site=KMDW&hours=168",
    "KAUS": "https://www.weather.gov/wrh/timeseries?site=KAUS&hours=168",
    "KMIA": "https://www.weather.gov/wrh/timeseries?site=KMIA&hours=168",
    "KDEN": "https://www.weather.gov/wrh/timeseries?site=KDEN&hours=168",
    "KPHL": "https://www.weather.gov/wrh/timeseries?site=KPHL&hours=168",
    "KLAX": "https://www.weather.gov/wrh/timeseries?site=KLAX&hours=168",
}

nws_site2kalshi_site = {
    "KNYC": "NY",
    "KMDW": "CHI",
    "KAUS": "AUS",
    "KMIA": "MIA",
    "KDEN": "DEN",
    "KPHL": "PHIL",
    "KLAX": "LAX"
}

kalshi_sites = ["NY", "CHI", "AUS", "MIA", "DEN", "PHIL", "LAX"]

kalshi_site2nws_site = {v:k for k,v in nws_site2kalshi_site.items()}

# each site reports a more accurate weather value
# on the hour of a varying minute
accurate_sensor_minute = {
    "KNYC": 51,
    "KMDW": 53,
    "KAUS": 53,
    "KMIA": 53,
    "KDEN": 53,
    "KPHL": 54,
    "KLAX": 53,
}

nws_site2tz = {
    "KNYC": "America/New_York",
    "KMDW": "America/Chicago",
    "KAUS": "America/Chicago",
    "KMIA": "America/New_York",
    "KDEN": "America/Denver",
    "KPHL": "America/New_York",
    "KLAX": "America/Los_Angeles"
}

nws_site2forecast = {
    "KNYC": "https://forecast.weather.gov/MapClick.php?lat=40.78&lon=-73.97&lg=english&&FcstType=digital",
    "KMDW": "https://forecast.weather.gov/MapClick.php?lat=41.78&lon=-87.76&lg=english&&FcstType=digital",
    "KAUS": "https://forecast.weather.gov/MapClick.php?lat=30.18&lon=-97.68&lg=english&&FcstType=digital",
    "KMIA": "https://forecast.weather.gov/MapClick.php?lat=25.7554&lon=-80.2262&lg=english&&FcstType=digital",
    "KDEN": "https://forecast.weather.gov/MapClick.php?lat=39.85&lon=-104.66&lg=english&&FcstType=digital",
    "KPHL": "https://forecast.weather.gov/MapClick.php?lat=40.08&lon=-75.01&lg=english&&FcstType=digital",
    "KLAX": "https://forecast.weather.gov/MapClick.php?lat=33.96&lon=-118.42&lg=english&&FcstType=digital",
}

if __name__ == "__main__":
    from utils import get_events_hardcoded
    from weather_sensor_reading import latest_sensor_reading
    site2days = get_events_hardcoded()

    for site in site2days.keys():
        nws_site = kalshi_site2nws_site[site]
        print(latest_sensor_reading(nws_site))