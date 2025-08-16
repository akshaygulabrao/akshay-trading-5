import re
import json
from typing import Dict, Any, Optional


def parse_climate_report(text: str) -> Dict[str, Any]:
    """
    Parse the Central Park climate report into JSON.
    """

    # Helper to find a single line and extract the last (right-most) number
    def extract_line_value(line: str, cast=float) -> Optional[Any]:
        parts = line.strip().split()
        if not parts:
            return None
        # Take the last token that can be cast
        for tok in reversed(parts):
            try:
                return cast(tok)
            except (ValueError, TypeError):
                continue
        return None

    # Helper to grab multiple numbers from a line using position
    def extract_line_fields(line: str, cast=float) -> Dict[str, Optional[Any]]:
        parts = line.strip().split()
        if len(parts) < 2:
            return {}
        # Assume the line has the structure: LABEL VAL TIME RECORD YEAR NORMAL DEPART LAST
        # We'll map by column index
        keys = ["value", "time", "record", "record_year", "normal", "departure", "last_year"]
        out = {}
        for i, k in enumerate(keys):
            if i + 1 < len(parts):
                val = parts[i + 1]
                if val in ("T", "MM"):
                    out[k] = None
                else:
                    try:
                        out[k] = cast(val)
                    except ValueError:
                        out[k] = val
            else:
                out[k] = None
        return out

    lines = text.splitlines()
    data: Dict[str, Any] = {
        "header": {},
        "temperature": {},
        "precipitation": {},
        "snowfall": {},
        "degree_days": {"heating": {}, "cooling": {}},
        "wind": {},
        "sky_cover": {},
        "weather_conditions": [],
        "relative_humidity": {},
        "normals_today": {},
        "sun": {},
    }

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        low = line.lower()

        if "CLIMATE REPORT" in line:
            # Header
            data["header"]["office"] = line
            i += 1
            continue

        # Temperature
        if low.startswith("maximum"):
            data["temperature"]["max"] = extract_line_fields(line)
        elif low.startswith("minimum"):
            data["temperature"]["min"] = extract_line_fields(line)
        elif low.startswith("average"):
            data["temperature"]["avg"] = extract_line_value(line)

        # Precipitation
        if low.startswith("yesterday") and "precipitation" in low:
            data["precipitation"]["yesterday"] = extract_line_fields(line)
        elif low.startswith("month to date") and "precipitation" in low:
            data["precipitation"]["month_to_date"] = extract_line_value(line)
        elif low.startswith("since jun 1") and "precipitation" in low:
            data["precipitation"]["since_jun1"] = extract_line_value(line)
        elif low.startswith("since jan 1") and "precipitation" in low:
            data["precipitation"]["since_jan1"] = extract_line_value(line)

        # Snowfall
        if low.startswith("yesterday") and "snowfall" in low:
            data["snowfall"]["yesterday"] = extract_line_fields(line)
        elif low.startswith("month to date") and "snowfall" in low:
            data["snowfall"]["month_to_date"] = extract_line_value(line)
        elif low.startswith("since jun 1") and "snowfall" in low:
            data["snowfall"]["since_jun1"] = extract_line_value(line)
        elif low.startswith("since jul 1") and "snowfall" in low:
            data["snowfall"]["since_jul1"] = extract_line_value(line)
        elif "snow depth" in low:
            data["snowfall"]["snow_depth"] = lines[i].split()[-1]

        # Degree days
        if low.startswith("yesterday") and "heating" in low:
            data["degree_days"]["heating"]["yesterday"] = extract_line_value(line)
        elif low.startswith("month to date") and "heating" in low:
            data["degree_days"]["heating"]["month_to_date"] = extract_line_value(line)
        elif low.startswith("since jun 1") and "heating" in low:
            data["degree_days"]["heating"]["since_jun1"] = extract_line_value(line)
        elif low.startswith("since jul 1") and "heating" in low:
            data["degree_days"]["heating"]["since_jul1"] = extract_line_value(line)

        if low.startswith("yesterday") and "cooling" in low:
            data["degree_days"]["cooling"]["yesterday"] = extract_line_value(line)
        elif low.startswith("month to date") and "cooling" in low:
            data["degree_days"]["cooling"]["month_to_date"] = extract_line_value(line)
        elif low.startswith("since jun 1") and "cooling" in low:
            data["degree_days"]["cooling"]["since_jun1"] = extract_line_value(line)
        elif low.startswith("since jan 1") and "cooling" in low:
            data["degree_days"]["cooling"]["since_jan1"] = extract_line_value(line)

        # Wind
        if "highest wind speed" in low:
            m = re.search(r"HIGHEST WIND SPEED\s+(\d+)", line, re.I)
            if m:
                data["wind"]["highest_speed_mph"] = int(m.group(1))
            m = re.search(r"HIGHEST WIND DIRECTION\s+([A-Z]+)", line, re.I)
            if m:
                data["wind"]["highest_dir"] = m.group(1)
        if "highest gust speed" in low:
            m = re.search(r"HIGHEST GUST SPEED\s+(\d+)", line, re.I)
            if m:
                data["wind"]["highest_gust_mph"] = int(m.group(1))
            m = re.search(r"HIGHEST GUST DIRECTION\s+([A-Z]+)", line, re.I)
            if m:
                data["wind"]["highest_gust_dir"] = m.group(1)
        if "average wind speed" in low:
            data["wind"]["avg_speed_mph"] = extract_line_value(line)

        # Sky cover
        if "average sky cover" in low:
            data["sky_cover"]["avg_cover"] = extract_line_value(line)

        # Weather conditions
        if "THE FOLLOWING WEATHER WAS RECORDED" in line:
            i += 1
            while i < len(lines) and lines[i].strip() and not lines[i].startswith("RELATIVE HUMIDITY"):
                w = lines[i].strip()
                if w and not w.startswith("---"):
                    data["weather_conditions"].append(w)
                i += 1
            continue

        # Relative humidity
        if "highest" in low and "relative humidity" in low:
            data["relative_humidity"]["highest"] = extract_line_value(line)
        elif "lowest" in low and "relative humidity" in low:
            data["relative_humidity"]["lowest"] = extract_line_value(line)
        elif "average" in low and "relative humidity" in low:
            data["relative_humidity"]["average"] = extract_line_value(line)

        # Normals for today
        if "climate normals for today" in low:
            i += 1
            while i < len(lines) and lines[i].strip():
                l = lines[i].strip()
                if "maximum temperature" in l.lower():
                    data["normals_today"]["max_temp"] = extract_line_value(l)
                elif "minimum temperature" in l.lower():
                    data["normals_today"]["min_temp"] = extract_line_value(l)
                i += 1
            continue

        # Sunrise / Sunset
        if "sunrise" in low and "sunset" in low:
            # Example line:
            # AUGUST 13 2025....SUNRISE   604 AM EDT   SUNSET   756 PM EDT
            m = re.match(
                r"""
                (?P<date>.+?)          # everything up to the dots
                \.{2,}                 # two or more dots (or other separators)
                SUNRISE\s+(?P<sunrise>[\d\sAP]+(?:AM|PM)\s+EDT).*
                SUNSET\s+(?P<sunset>[\d\sAP]+(?:AM|PM)\s+EDT)
                """,
                line,
                re.IGNORECASE | re.VERBOSE,
            )
            if m:
                data["sun"][m.group("date").strip()] = {
                    "sunrise": m.group("sunrise").strip(),
                    "sunset": m.group("sunset").strip(),
                }

        i += 1

    return data


# Example usage:
if __name__ == "__main__":
    import pprint

    raw = """193
CDUS41 KOKX 130627
CLINYC

CLIMATE REPORT
NATIONAL WEATHER SERVICE NEW YORK, NY
227 AM EDT WED AUG 13 2025

...................................

...THE CENTRAL PARK NY CLIMATE SUMMARY FOR AUGUST 12 2025...

CLIMATE NORMAL PERIOD 1991 TO 2020
CLIMATE RECORD PERIOD 1869 TO 2025


WEATHER ITEM   OBSERVED TIME   RECORD YEAR NORMAL DEPARTURE LAST
                VALUE   (LST)  VALUE       VALUE  FROM      YEAR
                                                  NORMAL
...................................................................
TEMPERATURE (F)
 YESTERDAY
  MAXIMUM         91    100 PM  97    1944  84      7       81
  MINIMUM         71    540 AM  55    1889  70      1       66
  AVERAGE         81                        77      4       74

PRECIPITATION (IN)
  YESTERDAY        0.00          3.62 1955   0.15  -0.15      T
  MONTH TO DATE    0.06                      1.86  -1.80     4.10
  SINCE JUN 1      6.55                     11.00  -4.45    10.01
  SINCE JAN 1     25.11                     30.17  -5.06    33.98

SNOWFALL (IN)
  YESTERDAY        0.0           0.0  2001   0.0    0.0      0.0
                                      2002
  MONTH TO DATE    0.0                       0.0    0.0      0.0
  SINCE JUN 1      0.0                       0.0    0.0      0.0
  SINCE JUL 1      0.0                       0.0    0.0      0.0
  SNOW DEPTH      MM

DEGREE DAYS
 HEATING
  YESTERDAY        0                         0      0        0
  MONTH TO DATE    0                         0      0        0
  SINCE JUN 1     16                        15      1        0
  SINCE JUL 1      0                         0      0        0

 COOLING
  YESTERDAY       16                        12      4        9
  MONTH TO DATE  126                       144    -18      151
  SINCE JUN 1    855                       758     97      922
  SINCE JAN 1    912                       839     73     1031
...................................................................


WIND (MPH)
  HIGHEST WIND SPEED     9   HIGHEST WIND DIRECTION    SE (140)
  HIGHEST GUST SPEED    21   HIGHEST GUST DIRECTION    SE (130)
  AVERAGE WIND SPEED     2.8


SKY COVER
  AVERAGE SKY COVER 0.0


WEATHER CONDITIONS
THE FOLLOWING WEATHER WAS RECORDED YESTERDAY.
  NO SIGNIFICANT WEATHER WAS OBSERVED.


RELATIVE HUMIDITY (PERCENT)
 HIGHEST    79           600 AM
 LOWEST     39          1200 PM
 AVERAGE    59

..........................................................


THE CENTRAL PARK NY CLIMATE NORMALS FOR TODAY
                         NORMAL    RECORD    YEAR
 MAXIMUM TEMPERATURE (F)   84        99      2005
 MINIMUM TEMPERATURE (F)   69        55      1930


SUNRISE AND SUNSET
AUGUST 13 2025........SUNRISE   604 AM EDT   SUNSET   756 PM EDT
AUGUST 14 2025........SUNRISE   605 AM EDT   SUNSET   755 PM EDT


-  INDICATES NEGATIVE NUMBERS.
R  INDICATES RECORD WAS SET OR TIED.
MM INDICATES DATA IS MISSING.
T  INDICATES TRACE AMOUNT.

$$"""

    parsed = parse_climate_report(raw)
    print(json.dumps(parsed, indent=2))

# {
#   "header": {
#     "office": "CLIMATE REPORT"
#   },
#   "temperature": {
#     "max": {
#       "value": 91.0,
#       "time": 100.0,
#       "record": "PM",
#       "record_year": 97.0,
#       "normal": 1944.0,
#       "departure": 84.0,
#       "last_year": 7.0
#     },
#     "min": {
#       "value": 71.0,
#       "time": 540.0,
#       "record": "AM",
#       "record_year": 55.0,
#       "normal": 1889.0,
#       "departure": 70.0,
#       "last_year": 1.0
#     },
#     "avg": 59.0
#   },
#   "precipitation": {},
#   "snowfall": {
#     "snow_depth": "MM"
#   },
#   "degree_days": {
#     "heating": {},
#     "cooling": {}
#   },
#   "wind": {
#     "highest_speed_mph": 9,
#     "highest_dir": "SE",
#     "highest_gust_mph": 21,
#     "highest_gust_dir": "SE",
#     "avg_speed_mph": 2.8
#   },
#   "sky_cover": {
#     "avg_cover": 0.0
#   },
#   "weather_conditions": [
#     "NO SIGNIFICANT WEATHER WAS OBSERVED."
#   ],
#   "relative_humidity": {},
#   "normals_today": {
#     "max_temp": 2005.0,
#     "min_temp": 1930.0
#   },
#   "sun": {
#     "AUGUST 13 2025": {
#       "sunrise": "604 AM EDT",
#       "sunset": "756 PM EDT"
#     },
#     "AUGUST 14 2025": {
#       "sunrise": "605 AM EDT",
#       "sunset": "755 PM EDT"
#     }
#   }
# }
