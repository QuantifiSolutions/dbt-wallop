WITH
ga_performance_final AS (
    SELECT
        p.adwords_adgroup_id,
        p.campaign,
        p.channel_grouping,
        p.source,
        p.source_medium,
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
        ON p.ga_account = g.ga_account
        AND p.ga_property = g.ga_property
        AND p.ga_view_number = g.ga_view_number
        AND p.adwords_adgroup_id = g.adwords_adgroup_id
        AND p.campaign = g.campaign
        AND p.channel_grouping = g.channel_grouping
        AND p.source = g.source
        AND p.source_medium = g.source_medium
        )

SELECT * FROM ga_performance_final