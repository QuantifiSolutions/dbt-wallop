WITH
processing_layer_1 AS (
    SELECT
        NULLIF(AdDescription, '') AS description1,
        NULLIF(AdDescription2, '') AS description2,
        NULLIF(CampaignId, '') AS campaign_id,
        NULLIF(CampaignName, '') AS campaign_name,
        CustomerId AS client_id,
        CustomerName AS client_name,
        NULLIF(DestinationUrl_utm_campaign, '') AS utm_campaign,
        NULLIF(DestinationUrl_utm_medium, '') AS utm_medium,
        NULLIF(DestinationUrl_utm_source, '') AS utm_source,
        Spend AS cost,
        Impressions AS impressions,
        Clicks AS clicks,
        DATE(TimePeriod) AS `date`,
        account AS account_id,
    FROM `bigquery-312020`.`analytics_wallop`.`pma_Bing_Ads_Performance_Ad_Level_`
    ),

final AS (
    SELECT
        account_id,
        campaign_id,
        campaign_name,
        utm_campaign,
        utm_medium,
        utm_source,
        description1,
        description2,
        NULLIF(ARRAY_TO_STRING([description1, description2], '\n---\n'), '') AS description,
        cost,
        impressions,
        clicks,
        date,
        client_id,
        client_name,
    FROM processing_layer_1
    WHERE
        campaign_id IS NOT NULL
        )

SELECT * FROM final