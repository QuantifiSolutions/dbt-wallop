

  create or replace view `dbt-wallop-dev-1`.`google_analytics`.`stg_ga__goals`
  OPTIONS()
  as WITH
ga_goals AS (
    SELECT
        NULLIF(COALESCE(g1.adwordsAdGroupID, g2.adwordsAdGroupID), '(not set)') AS adwords_adgroup_id,
        NULLIF(COALESCE(g1.campaign, g2.campaign), '(not set)') AS campaign,
        COALESCE(g1.channelGrouping, g2.channelGrouping) AS channel_grouping,
        COALESCE(g1.source, g2.source) AS source,
        COALESCE(g1.sourceMedium, g2.sourceMedium) AS source_medium,
        IFNULL(g1.goal1Completions, 0) AS goal1_completions,
        IFNULL(g1.goal2Completions, 0) AS goal2_completions,
        IFNULL(g1.goal3Completions, 0) AS goal3_completions,
        IFNULL(g1.goal4Completions, 0) AS goal4_completions,
        IFNULL(g1.goal5Completions, 0) AS goal5_completions,
        IFNULL(g1.goal6Completions, 0) AS goal6_completions,
        IFNULL(g1.goal7Completions, 0) AS goal7_completions,
        IFNULL(g1.goal8Completions, 0) AS goal8_completions,
        IFNULL(g1.goal9Completions, 0) AS goal9_completions,
        IFNULL(g1.goal10Completions, 0) AS goal10_completions,
        IFNULL(g2.goal11Completions, 0) AS goal11_completions,
        IFNULL(g2.goal12Completions, 0) AS goal12_completions,
        IFNULL(g2.goal13Completions, 0) AS goal13_completions,
        IFNULL(g2.goal14Completions, 0) AS goal14_completions,
        IFNULL(g2.goal15Completions, 0) AS goal15_completions,
        IFNULL(g2.goal16Completions, 0) AS goal16_completions,
        IFNULL(g2.goal17Completions, 0) AS goal17_completions,
        IFNULL(g2.goal18Completions, 0) AS goal18_completions,
        IFNULL(g2.goal19Completions, 0) AS goal19_completions,
        IFNULL(g2.goal20Completions, 0) AS goal20_completions,
        DATE(COALESCE(g1.date, g2.date)) AS `date`,
        COALESCE(g1.configName, g2.configName) AS config_name,
        REGEXP_EXTRACT(COALESCE(g1.configName, g2.configName), "(?i)^([a-z0-9\\(\\)\\-\\./': é&]+) -> .*$") AS ga_account,
        REGEXP_EXTRACT(COALESCE(g1.configName, g2.configName), "(?i)^.* -> ([a-z0-9\\(\\)\\-\\./': é&]+) -> .*$") AS ga_property,
        REGEXP_EXTRACT(COALESCE(g1.configName, g2.configName), "(?i)^.* -> .* -> ([a-z0-9\\(\\)\\-\\./': é&]+) \\([0-9]+\\)$") AS ga_view_name,
        REGEXP_EXTRACT(COALESCE(g1.configName, g2.configName), ".* -> .* -> .* \\(([0-9]+)\\)$") AS ga_view_number
    FROM `bigquery-312020`.`analytics_wallop`.`pma_Google_Analytics_UA_Goals_1_10_*` g1
    FULL OUTER JOIN `bigquery-312020`.`analytics_wallop`.`pma_Google_Analytics_UA_Goals_11_20_*` g2
        ON g1.adwordsAdGroupID = g2.adwordsAdGroupID
        AND g1.campaign = g2.campaign
        AND g1.source = g2.source
        AND g1.sourceMedium = g2.sourceMedium
        AND g1.date = g2.date
        AND g1.configName = g2.configName
        )

SELECT * FROM ga_goals;

