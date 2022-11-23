WITH
ga_performance_final AS (
    SELECT
        p.adwords_adgroup_id,
        p.channel_grouping,
        p.campaign AS utm_campaign,
        p.source AS utm_source,
        SPLIT(p.source_medium, ' / ')[OFFSET(1)] AS utm_medium, 
        p.cost,
        p.impressions,
        p.clicks,
        p.sessions,
        p.session_duration,
        p.pageviews,
        p.bounces,
        p.transactions,
        p.transaction_revenue,
        p.goal_completions_all,
        g.goal1_completions,
        g.goal2_completions,
        g.goal3_completions,
        g.goal4_completions,
        g.goal5_completions,
        g.goal6_completions,
        g.goal7_completions,
        g.goal8_completions,
        g.goal9_completions,
        g.goal10_completions,
        g.goal11_completions,
        g.goal12_completions,
        g.goal13_completions,
        g.goal14_completions,
        g.goal15_completions,
        g.goal16_completions,
        g.goal17_completions,
        g.goal18_completions,
        g.goal19_completions,
        g.goal20_completions,
        p.date,
        p.ga_account,
        p.ga_property,
        p.ga_view_name,
        p.ga_view_number,
    FROM `dbt-wallop-dev-1`.`google_analytics`.`stg_ga__performance` p
    LEFT JOIN `dbt-wallop-dev-1`.`google_analytics`.`stg_ga__goals` g
        ON p.config_name = g.config_name
        AND IFNULL(p.adwords_adgroup_id, '') = IFNULL(g.adwords_adgroup_id, '')
        AND IFNULL(p.campaign, '') = IFNULL(g.campaign, '')
        AND p.channel_grouping = g.channel_grouping
        AND p.source = g.source
        AND p.source_medium = g.source_medium
        AND p.date = g.date
        )

SELECT * FROM ga_performance_final