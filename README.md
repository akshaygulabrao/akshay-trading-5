# Weather Tables

1. NY,https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC, (40.78,-73.97), https://www.weather.gov/wrh/timeseries?site=KNYC&hours=168
2. CHI,https://forecast.weather.gov/product.php?site=LOT&product=CLI&issuedby=MDW, (41.78, -87.76)
3. AUS,https://forecast.weather.gov/product.php?site=EWX&product=CLI&issuedby=AUS, (30.18,-97.68)
4. MIA, https://forecast.weather.gov/product.php?site=MFL&product=CLI&issuedby=MIA, (25.79, -80.31)
5. DEN, https://forecast.weather.gov/product.php?site=BOU&product=CLI&issuedby=DEN, (39.85,-104.66), 
6. PHIL,https://forecast.weather.gov/product.php?site=PHI&product=CLI&issuedby=PHL, (40.08,-75.01),https://www.weather.gov/wrh/timeseries?site=KPHL&hours=168
7. LAX,https://forecast.weather.gov/product.php?site=LOX&product=CLI&issuedby=LAX, (33.94,-118.39),https://www.weather.gov/wrh/timeseries?site=KLAX&hours=168


https://api.synopticdata.com/v2/stations/latest?bbox=-112.291260,40.544070,-111.593628,40.972640&vars=air_temp&token=d8c6aee36a994f90857925cea26934be


https://forecast.weather.gov/MapClick.php?lat=33.951&lon=-118.418&lg=english&&FcstType=digital



contract_price(time_till_expiration,forecast_max(t0), latest_observations(t1..5),yes_values)
    if forecast_max > yes_values:
        contract_price = 0
    if latest_observations[5] - latest_observation[4] < 0 and is yes_value:
        contract_price = 1
