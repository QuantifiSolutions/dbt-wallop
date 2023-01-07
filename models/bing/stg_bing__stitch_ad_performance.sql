WITH
unioned_bing AS (
  SELECT * FROM {{source('wallop_creative_bing_ads', 'campaign_performance_report')}}
  UNION ALL
  SELECT * FROM {{source('wallop_creative_bing_ads_carmel', 'campaign_performance_report')}}
  UNION ALL
  SELECT * FROM {{source('wallop_creative_bing_ads_hutton', 'campaign_performance_report')}}
  ),

-- cast stitch columns to the pma format, remove null campaign ids
processing_layer_1 AS (
    SELECT
        _sdc_report_datetime,
        accountid AS account_id,
        customername AS account_name,
        campaignid AS campaign_id,
        campaignname AS campaign_name,
        LOWER(campaignname) AS utm_campaign,
        'cpc' AS utm_medium,
        'bing' AS utm_source,
        CAST(NULL AS STRING) AS description1,
        CAST(NULL AS STRING) AS description2,
        CAST(NULL AS STRING) AS description,
        spend AS cost,
        impressions,
        clicks,
        DATE(timeperiod) AS `date`,
    FROM unioned_bing
    WHERE
        campaignid IS NOT NULL
        ),

-- create composite primary key based on row except _sdc_report_datetime
processing_layer_2 AS (
  SELECT
      *,
      CONCAT(account_id, campaign_id, date) AS pk
  FROM processing_layer_1 t),


-- create ranking based on _sdc_report_datetime for rows over primary key
processing_layer_3 AS (
  SELECT
      *,
      RANK() OVER (PARTITION BY pk ORDER BY _sdc_report_datetime DESC) AS rk
  FROM processing_layer_2),

-- keep latest row based on rank = 1, remove ranking cols
processing_layer_4 AS (
    SELECT
        p3.* EXCEPT (rk, pk, _sdc_report_datetime),
    FROM processing_layer_3 p3
    WHERE
        rk = 1
        ),

-- table with row_uuid
final AS (
    SELECT
        *,
        FARM_FINGERPRINT(TO_JSON_STRING(p4)) AS row_uuid
    FROM processing_layer_4 p4
    )

SELECT * FROM final
