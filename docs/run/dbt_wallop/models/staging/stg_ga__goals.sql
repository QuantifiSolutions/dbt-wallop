

  create or replace view `dbt-wallop-dev-1`.`staging`.`stg_ga__goals`
  OPTIONS()
  as WITH
ga_goals AS (
    SELECT
        NULLIF(g1.adwordsAdGroupID, '(not set)') AS adwords_adgroup_id,
        NULLIF(g1.campaign, '(not set)') AS campaign,
        g1.channelGrouping AS channel_grouping,
        g1.source,
        g1.sourceMedium AS source_medium,
        g1.goal1Completions AS goal1_completions,
        g1.goal2Completions AS goal2_completions,
        g1.goal3Completions AS goal3_completions,
        g1.goal4Completions AS goal4_completions,
        g1.goal5Completions AS goal5_completions,
        g1.goal6Completions AS goal6_completions,
        g1.goal7Completions AS goal7_completions,
        g1.goal8Completions AS goal8_completions,
        g1.goal9Completions AS goal9_completions,
        g1.goal10Completions AS goal10_completions,
        g2.goal11Completions AS goal11_completions,
        g2.goal12Completions AS goal12_completions,
        g2.goal13Completions AS goal13_completions,
        g2.goal14Completions AS goal14_completions,
        g2.goal15Completions AS goal15_completions,
        g2.goal16Completions AS goal16_completions,
        g2.goal17Completions AS goal17_completions,
        g2.goal18Completions AS goal18_completions,
        g2.goal19Completions AS goal19_completions,
        g2.goal20Completions AS goal20_completions,
        DATE(g1.date) AS `date`,
        g1.configName AS config_name,
        REGEXP_EXTRACT(g1.configName, "(?i)^([a-z0-9\\(\\)\\-\\./': é]+) -> .*$") AS ga_account,
        REGEXP_EXTRACT(g1.configName, "(?i)^.* -> ([a-z0-9\\(\\)\\-\\./': é]+) -> .*$") AS ga_property,
        REGEXP_EXTRACT(g1.configName, "(?i)^.* -> .* -> ([a-z0-9\\(\\)\\-\\./': é]+) \\([0-9]+\\)$") AS ga_view_name,
        REGEXP_EXTRACT(g1.configName, ".* -> .* -> .* \\(([0-9]+)\\)$") AS ga_view_number
    FROM `bigquery-312020`.`analytics_wallop`.`pma_Google_Analytics_UA_Goals_1_10_*` g1
    JOIN `bigquery-312020`.`analytics_wallop`.`pma_Google_Analytics_UA_Goals_11_20_*` g2
        ON g1.adwordsAdGroupID = g2.adwordsAdGroupID
        AND g1.campaign = g2.campaign
        AND g1.source = g2.source
        AND g1.date = g2.date
        AND g1.configName = g2.configName
        )

SELECT * FROM ga_goals;

