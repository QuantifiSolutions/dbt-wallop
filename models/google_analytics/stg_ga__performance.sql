WITH
-- union google analytics main and separate client sources
processing_layer_1 AS (
    SELECT * FROM {{source('analytics_wallop', 'pma_google_analytics_ua_performance')}}
    UNION ALL
    SELECT * FROM {{source('analytics_wallop', 'pma_google_analytics_ua_performance_fairmont')}}
    ),

-- Select distinct in case there exists overlap between google analytics main and seperate client sources
final AS (
    SELECT DISTINCT
        NULLIF(adwordsAdGroupID, '(not set)') AS adwords_adgroup_id,
        NULLIF(campaign, '(not set)') AS campaign,
        channelGrouping AS channel_grouping,
        source,
        sourceMedium AS source_medium,
        adCost AS cost,
        impressions,
        adClicks AS clicks,
        sessions,
        sessionDuration AS session_duration,
        pageviews,
        bounces,
        transactions,
        transactionRevenue AS transaction_revenue,
        goalCompletionsAll AS goal_completions_all,
        DATE(date) AS `date`,
        configName AS config_name,
        REGEXP_EXTRACT(configName, "(?i)^([a-z0-9\\(\\)\\-\\./': é&]+) -> .*$") AS ga_account,
        REGEXP_EXTRACT(configName, "(?i)^.* -> ([a-z0-9\\(\\)\\-\\./': é&]+) -> .*$") AS ga_property,
        REGEXP_EXTRACT(configName, "(?i)^.* -> .* -> ([a-z0-9\\(\\)\\-\\./': é&]+) \\([0-9]+\\)$") AS ga_view_name,
        REGEXP_EXTRACT(configName, ".* -> .* -> .* \\(([0-9]+)\\)$") AS ga_view_number
    FROM processing_layer_1
    )

SELECT * FROM final
