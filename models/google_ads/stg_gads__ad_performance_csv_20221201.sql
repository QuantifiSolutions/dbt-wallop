WITH
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
    FROM {{source('analytics_wallop', 'pma_google_ads_performance_ad_level_csv_upload_20221201')}}
    WHERE
        campaign_id != ''
        )

SELECT * FROM processing_layer_1
