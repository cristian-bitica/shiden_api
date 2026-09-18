"""Silver layer processors — Kimball star schema.

Dimensions (static / semi-static, built first)::

    dim_date            one row per calendar day; includes EUR/RON FX rate
    dim_datetime        one row per settlement interval per IANA timezone
                        (DST-aware: 92/96/100 rows per local day)
    dim_market          one row per market; from config registry
    dim_production_type ENTSO-E type → category mapping
    dim_location        weather measurement points; from config registry

Facts (built after dimensions)::

    fact_price          15-min × market; OPCOM PZU EUR-denominated prices
    fact_generation     1-hour × production_type × market; ENTSO-E A75
    fact_load           1-hour × market; ENTSO-E A65
    fact_weather        1-hour × location; Open-Meteo

Fact tables key on the UTC instant -- ``timestamp_utc`` (fact_price) or
``hour_start_utc`` (the hourly facts).  Local columns (``date_id``,
``local_time_id``, ``time_label``) are carried as labels so delivery-day
semantics still match OPCOM's interval numbering, but they are never merge
keys: on the autumn changeover local 03:00 occurs twice, so a local key
silently collapses two distinct hours into one.  See ``shiden.timeaxis``.
"""
