

  create or replace view `dbt-wallop-dev-1`.`google_ads`.`stg_gads__ad_performance_csv_20221201`
  OPTIONS()
  as WITH
-- order of the columns needs to match pma source, rename columns, and remove blank campaign_id rows
processing_layer_1 AS (
    SELECT
        account_id,
        ad_group_id,
        ad_type,
        ad_group_name AS adgroup_name,
        campaign_id,
        campaign_name,
        clicks,
        cost,
        DATETIME(PARSE_DATE('%Y/%m/%d', `date`)) AS `date`,
        description1,
        description2,
        headline1,
        headline2,
        headline3,
        impressions,
        account_name AS configName,
    FROM `bigquery-312020`.`analytics_wallop`.`pma_Google_Ads_Performance_Ad_Level_CSV_Upload_20221201`
    WHERE
        campaign_id != ''
        )

SELECT * FROM processing_layer_1;

