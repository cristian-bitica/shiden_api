"""Silver layer processors — Kimball star schema.

Dimensions (static / semi-static, built first):
    dim_date            one row per calendar day; includes EUR/RON FX rate
    dim_datetime        one row per settlement interval per market
                        (DST-aware: 92/96/100 rows per local day)
    dim_market          one row per market; from config registry
    dim_production_type ENTSO-E type → category mapping
    dim_location        weather measurement points; from config registry

Facts (built after dimensions):
    fact_price          15-min × market; OPCOM PZU EUR-denominated prices
    fact_generation     1-hour × production_type × market; ENTSO-E A75
    fact_load           1-hour × market; ENTSO-E A65
    fact_weather        1-hour × location; Open-Meteo

All fact tables use LOCAL time (Europe/Bucharest) for date_id and time_id so
that delivery-day semantics match OPCOM's interval numbering.  ENTSO-E and
weather UTC timestamps are converted to local before key assignment.
"""
