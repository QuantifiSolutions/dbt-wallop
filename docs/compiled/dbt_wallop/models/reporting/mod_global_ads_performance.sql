WITH
gads_core AS (
    SELECT
        'Google Ads' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS ad_name,
        'google' AS utm_source,
        'cpc' AS utm_medium,
        campaign_name AS utm_campaign,
        headline,
        description,
        cost,
        impressions,
        clicks,
        date,
    FROM `dbt-wallop-dev-1`.`google_ads`.`stg_gads__ad_performance`
    ),

fb_core AS (
    SELECT
        'Facebook' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        utm_source,
        utm_medium,
        utm_campaign,
        headline,
        description,
        cost,
        impressions,
        clicks,
        date,
    FROM `dbt-wallop-dev-1`.`facebook`.`stg_fb__ad_performance`
    ),

bing_core AS (
    SELECT
        'Bing' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        CAST(NULL AS STRING) AS adgroup_id,
        CAST(NULL AS STRING) AS adgroup_name,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS ad_name,
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(NULL AS STRING) AS headline,
        description,
        cost,
        impressions,
        clicks,
        date,
    FROM `dbt-wallop-dev-1`.`bing`.`stg_bing__ad_performance`
    ),

unioned AS (
    SELECT * FROM gads_core

    UNION ALL

    SELECT * FROM fb_core

    UNION ALL

    SELECT * FROM bing_core
    )

SELECT * FROM unioned