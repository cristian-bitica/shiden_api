import json
import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Literal

from shiden.config.markets import get_market
from shiden.config.settings import settings
from shiden.ingestion.http import get_text_with_retry
from shiden.landing_paths import weather_hourly_json_path

logger = logging.getLogger(__name__)

# Open-Meteo source URL template stored in each landing file for lineage.
# The endpoint (archive vs forecast) is embedded so Bronze can read back the
# exact URL that was used — avoids reconstructing it from a hardcoded template.
_SOURCE_URL_TMPL = (
    "{endpoint}"
    "?latitude={lat}&longitude={lon}"
    "&start_date={start}&end_date={end}"
    "&hourly=temperature_2m,wind_speed_10m,wind_speed_100m,shortwave_radiation,cloud_cover"
    "&timezone=UTC"
)

# Open-Meteo — free, no API key required
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variables relevant to BESS charge/discharge decisions:
#   - temperature_2m       → demand proxy (heating/cooling load)
#   - wind_speed_10m/100m  → wind generation proxy
#   - shortwave_radiation  → solar generation proxy
#   - cloud_cover          → solar generation proxy
HOURLY_VARIABLES = [
    "temperature_2m",
    "wind_speed_10m",
    "wind_speed_100m",
    "shortwave_radiation",
    "cloud_cover",
]


class WeatherIngester:
    """
    Ingests hourly weather data from Open-Meteo for all configured
    locations in a market. No API key required.
    """

    def fetch(self, market_id: str, start: date, end: date) -> dict[str, Any]:
        market = get_market(market_id)
        # Use historical archive for completed past days; forecast for today or
        # future. Strict less-than means a range ending today hits forecast,
        # which covers recent history equally well and avoids a 404 on archive.
        url = (
            OPEN_METEO_HISTORICAL_URL if end < date.today() else OPEN_METEO_FORECAST_URL
        )
        results = []
        for location in market.weather_locations:
            params: dict[str, str | float] = {
                "latitude": location.lat,
                "longitude": location.lon,
                "hourly": ",".join(HOURLY_VARIABLES),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "timezone": "UTC",
            }
            data = json.loads(
                get_text_with_retry(
                    url, params=params, timeout=30, source="Open-Meteo"
                )
            )
            if "time" not in data.get("hourly", {}):
                logger.warning(
                    "Weather fetch: unexpected response shape for %s / %s"
                    " — skipping",
                    market_id,
                    location.name,
                )
                continue
            results.append({"location": location.name, "data": data, "url": url})
            logger.info(
                "Weather fetch: market=%s location=%s start=%s end=%s hours=%d",
                market_id,
                location.name,
                start.isoformat(),
                end.isoformat(),
                len(data["hourly"]["time"]),
            )

        return {
            "market_id": market_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "locations": results,
        }

    def ingest(self, market_id: str, start: date, end: date) -> None:
        """
        Fetch raw weather JSON for all locations and save to the landing zone.

        Landing path:
            {landing_base_path}/landing/weather/{market_id}/{location}/{YYYY-MM-DD}.json
        One file per location per day. Includes source_url for Bronze lineage.
        """
        # Fetch the full range per location in one API call (Open-Meteo supports ranges)
        payload = self.fetch(market_id, start, end)
        saved_count = 0

        for loc_data in payload["locations"]:
            location_name = loc_data["location"]
            hourly = loc_data["data"]["hourly"]
            times = hourly["time"]
            endpoint_url = loc_data["url"]

            # Group hours by date and write one file per (location, date)
            by_date: dict[str, list[int]] = defaultdict(list)
            for i, ts in enumerate(times):
                by_date[datetime.fromisoformat(ts).date().isoformat()].append(i)

            for day_str, indices in by_date.items():
                lat = loc_data["data"]["latitude"]
                lon = loc_data["data"]["longitude"]
                day_hourly: dict[str, Any] = {
                    var: [hourly[var][i] for i in indices]
                    for var in ["time"] + HOURLY_VARIABLES
                }
                source_url = _SOURCE_URL_TMPL.format(
                    endpoint=endpoint_url,
                    lat=lat,
                    lon=lon,
                    start=day_str,
                    end=day_str,
                )
                # REALIZED = date is in the past (actual weather occurred).
                # FORECAST = date is today or future (model output, not yet verified).
                # ML training uses REALIZED; inference uses FORECAST.
                data_type: Literal["REALIZED", "FORECAST"] = (
                    "REALIZED" if day_str < date.today().isoformat() else "FORECAST"
                )
                payload_for_day = {
                    "market_id": market_id,
                    "location": location_name,
                    "lat": lat,
                    "lon": lon,
                    "date": day_str,
                    "data_type": data_type,
                    "fetched_at": payload["fetched_at"],
                    "source_url": source_url,
                    "hourly": day_hourly,
                }

                landing_path = weather_hourly_json_path(
                    settings.landing_base_path,
                    market_id,
                    location_name,
                    date.fromisoformat(day_str),
                )
                landing_path.parent.mkdir(parents=True, exist_ok=True)
                landing_path.write_text(
                    json.dumps(payload_for_day, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                saved_count += 1
                logger.info("Weather landing: saved %s", landing_path)

        logger.info(
            "Weather ingest complete: market=%s start=%s end=%s files=%d",
            market_id,
            start.isoformat(),
            end.isoformat(),
            saved_count,
        )
