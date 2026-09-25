# Appendix

```{toctree}
:maxdepth: 1

../silver_layer_instructions
../bnr_ecb_ingestion_instructions
```

## Original build specifications

The two documents below are the original, task-scoped build specifications
written before the Silver layer and the FX ingesters existed. They are kept
because they record the *reasoning* behind decisions the finished code can only
show the outcome of — why exchange rates are forward-filled rather than
interpolated, why BNR is primary and ECB the fallback, why the star schema has
no snowflaking.

They are **historical**. Where they disagree with {doc}`../silver_schema` or
{doc}`../architecture`, those pages are authoritative — most notably, the
static `dim_time` they describe has been replaced by `dim_datetime`
({doc}`../time-model`).
