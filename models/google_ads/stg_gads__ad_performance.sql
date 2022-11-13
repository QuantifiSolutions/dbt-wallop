WITH
processing_layer_1 AS (
    SELECT
        NULLIF(account_id, '') AS account_id,
        NULLIF(ad_group_id, '') AS adgroup_id,
        NULLIF(adgroup_name, '') AS adgroup_name,
        NULLIF(ad_type, '') AS ad_type,
        NULLIF(campaign_id, '') AS campaign_id,
        NULLIF(campaign_name, '') AS campaign_name,
        NULLIF(description1, '') AS description1,
        NULLIF(description2, '') AS description2,
        NULLIF(headline1, '') AS headline1,
        NULLIF(headline2, '') AS headline2,
        NULLIF(headline3, '') AS headline3,
        cost,
        impressions,
        clicks,
        DATE(date) AS `date`,
        configName AS client_name,
    FROM {{source('analytics_wallop', 'pma_google_ads_performance_ad_level')}}
    ),

final AS (
    SELECT
        account_id,
        adgroup_id,
        adgroup_name,
        ad_type,
        campaign_id,
        campaign_name,
        description1,
        description2,
        NULLIF(ARRAY_TO_STRING([description1, description2], '\n---\n'), '') AS description,
        headline1,
        headline2,
        headline3,
        NULLIF(ARRAY_TO_STRING([headline1, headline2, headline3], '\n---\n'), '') AS headline,
        cost,
        impressions,
        clicks,
        `date`,
        client_name,
    FROM processing_layer_1
    WHERE
        campaign_id IS NOT NULL
        )

SELECT * FROM final
