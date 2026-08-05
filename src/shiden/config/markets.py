from dataclasses import dataclass, field


@dataclass(frozen=True)
class WeatherLocation:
    name: str
    lat: float
    lon: float
    # Signal weights for Gold-layer national aggregation
    # (each set sums to 1.0 across locations).
    # Approximate — based on installed capacity distribution and population.
    # Refine when actual ANRE/Transelectrica capacity data is available.
    wind_weight: float = 0.0  # contribution to national wind generation proxy
    solar_weight: float = 0.0  # contribution to national solar/prosumer proxy
    demand_weight: float = 0.0  # contribution to national demand (temperature) proxy


@dataclass(frozen=True)
class MarketConfig:
    bidding_zone: str  # ENTSO-E EIC code
    timezone: str
    currency: str  # local settlement currency (RON, GBP, EUR)
    name: str = ""  # human-readable market name (falls back to market_id)
    reporting_currency: str = "EUR"  # target conversion currency for Silver prices
    fx_source: str = "BNR"  # primary FX source: BNR | ECB | SYNTHETIC
    weather_locations: tuple[WeatherLocation, ...] = field(default_factory=tuple)


MARKETS: dict[str, MarketConfig] = {
    "RO": MarketConfig(
        bidding_zone="10YRO-TEL------P",
        timezone="Europe/Bucharest",
        currency="RON",
        name="Romania",
        reporting_currency="EUR",
        fx_source="BNR",  # BNR authoritative Romanian rate; ECB for cross-validation
        weather_locations=(
            # Bucharest — largest demand centre; Muntenia south solar prosumers
            WeatherLocation(
                "Bucharest",
                44.4268,
                26.1025,
                wind_weight=0.02,
                solar_weight=0.22,
                demand_weight=0.35,
            ),
            # Dobrogea (Constanța) — dominant wind corridor (~70% of RO installed wind);
            # coastal solar farms
            WeatherLocation(
                "Dobrogea",
                44.1800,
                28.6500,
                wind_weight=0.70,
                solar_weight=0.18,
                demand_weight=0.08,
            ),
            # Oltenia (Craiova) — CE Oltenia coal; SW solar farms; Olt river hydro
            WeatherLocation(
                "Oltenia",
                44.3200,
                23.8000,
                wind_weight=0.02,
                solar_weight=0.17,
                demand_weight=0.15,
            ),
            # Transylvania (Cluj-Napoca) — hydro proxy (Someș/Arieș basins);
            # central/NW demand
            WeatherLocation(
                "Transylvania",
                46.7712,
                23.5938,
                wind_weight=0.05,
                solar_weight=0.15,
                demand_weight=0.20,
            ),
            # Moldova (Iași) — NE region; growing wind capacity;
            # distributed prosumer solar
            WeatherLocation(
                "Moldova",
                47.1585,
                27.5907,
                wind_weight=0.11,
                solar_weight=0.15,
                demand_weight=0.12,
            ),
            # Banat (Timișoara) — W region; Banat wind farms;
            # significant industrial demand
            WeatherLocation(
                "Banat",
                45.7489,
                21.2087,
                wind_weight=0.10,
                solar_weight=0.13,
                demand_weight=0.10,
            ),
        ),
    ),
    # Add markets here — zero code changes elsewhere
    # "DE": MarketConfig(
    #     bidding_zone="10Y1001A1001A83F",
    #     timezone="Europe/Berlin",
    #     currency="EUR",
    #     reporting_currency="EUR",
    #     fx_source="SYNTHETIC",  # EUR market — Silver materialises rate=1.0 row
    #     weather_locations=(...),
    # ),
    # "GB": MarketConfig(
    #     bidding_zone="10YGB----------A",
    #     timezone="Europe/London",
    #     currency="GBP",
    #     reporting_currency="EUR",
    #     fx_source="ECB",  # ECB provides EUR/GBP
    #     weather_locations=(...),
    # ),
}


def get_market(market_id: str) -> MarketConfig:
    if market_id not in MARKETS:
        raise ValueError(
            f"Unknown market: '{market_id}'. Available: {list(MARKETS.keys())}"
        )
    return MARKETS[market_id]
