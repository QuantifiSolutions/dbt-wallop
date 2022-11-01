WITH
ga_performance AS (
    SELECT
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
        transactionRevenue AS transaction_revenue,
        transactions,
        goalCompletionsAll AS goal_completions_all,
        DATE(date) AS `date`,
        configName AS config_name,
        REGEXP_EXTRACT(configName, "(?i)^([a-z0-9\\(\\)\\-\\./': é]+) -> .*$") AS ga_account,
        REGEXP_EXTRACT(configName, "(?i)^.* -> ([a-z0-9\\(\\)\\-\\./': é]+) -> .*$") AS ga_property,
        REGEXP_EXTRACT(configName, "(?i)^.* -> .* -> ([a-z0-9\\(\\)\\-\\./': é]+) \\([0-9]+\\)$") AS ga_view_name,
        REGEXP_EXTRACT(configName, ".* -> .* -> .* \\(([0-9]+)\\)$") AS ga_view_number
    FROM `bigquery-312020`.`analytics_wallop`.`pma_Google_Analytics_UA_Performance_*`
    )

SELECT * FROM ga_performance