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
