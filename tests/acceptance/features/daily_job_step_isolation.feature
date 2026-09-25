# mutation-stamp: sha256=844d1dbe6b5a934829bfed1953c8505bcf1cb0eecfb031e862d09b929a317cb0
# acceptance-mutation-manifest-begin
# {"version":1,"tested_at":"2026-09-25T19:04:13.653398601Z","feature_name":"Daily pipeline job step error isolation","feature_path":"tests/acceptance/features/daily_job_step_isolation.feature","background_hash":"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b","implementation_hash":"unknown","scenarios":[{"index":0,"name":"daily-job-step-isolation-1","scenario_hash":"baab66442973ef0e373d1be3aec140b2b628bf12f050cdc3f7cd1d053606b685","mutation_count":6,"result":{"Total":6,"Killed":6,"Survived":0,"Errors":0},"tested_at":"2026-09-25T19:04:13.653398601Z"}]}
# acceptance-mutation-manifest-end

# daily-job-step-isolation-1: a failing pipeline step is logged, not raised
Feature: Daily pipeline job step error isolation

  Scenario Outline: daily-job-step-isolation-1
    Given a pipeline step inside daily job "<job>" raises an exception
    When daily job "<job>" runs
    Then daily job "<job>" logs an ERROR for the failing step
    And daily job "<job>" completes without raising

    Examples:
      | job                 |
      | run_opcom_pzu_daily |
      | run_weather_daily   |
      | run_entsoe_daily    |
      | run_fx_rates_daily  |
      | run_silver_daily    |
      | run_gold_daily      |
