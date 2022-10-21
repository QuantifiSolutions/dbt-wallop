

  create or replace view `dbt-wallop-dev-1`.`example`.`test_bigquery`
  OPTIONS()
  as SELECT * FROM `bigquery-312020.production.google_ads_performance`;

