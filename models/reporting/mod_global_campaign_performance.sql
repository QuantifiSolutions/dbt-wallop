{{ config(
    materialized = 'table'
)}}

WITH
campaign_rollup AS (
    SELECT
        data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        utm_source,
        utm_medium,
        utm_campaign,
        date,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        STRUCT(
            SUM(fb.actions_link_click) AS actions_link_click,
            SUM(fb.actions_onsite_conversion__post_save) AS actions_onsite_conversion__post_save,
            SUM(fb.actions_post) AS actions_post,
            SUM(fb.actions_post_engagement) AS actions_post_engagement,
            SUM(fb.actions_post_reaction) AS actions_post_reaction,
            SUM(fb.actions_purchase) AS actions_purchase,
            SUM(fb.actions_video_view_video_play) AS actions_video_view_video_play
        ) fb
    FROM {{ref('mod_global_ad_performance')}}
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9
        ),

ga_campaign_rollup AS (
    SELECT
        'Google Analytics' AS data_source,
        ga_account AS account_name,
        channel_grouping,
        utm_campaign,
        utm_source,
        utm_medium,
        date,
        STRUCT(
            SUM(sessions) AS sessions,
            SUM(session_duration) AS session_duration,
            SUM(pageviews) AS pageviews,
            SUM(bounces) AS bounces,
            SUM(transactions) AS transactions,
            SUM(transaction_revenue) AS transaction_revenue,
            SUM(goal_completions_all) AS goal_completions_all,
            SUM(goal1_completions) AS goal1_completions,
            SUM(goal2_completions) AS goal2_completions,
            SUM(goal3_completions) AS goal3_completions,
            SUM(goal4_completions) AS goal4_completions,
            SUM(goal5_completions) AS goal5_completions,
            SUM(goal6_completions) AS goal6_completions,
            SUM(goal7_completions) AS goal7_completions,
            SUM(goal8_completions) AS goal8_completions,
            SUM(goal9_completions) AS goal9_completions,
            SUM(goal10_completions) AS goal10_completions,
            SUM(goal11_completions) AS goal11_completions,
            SUM(goal12_completions) AS goal12_completions,
            SUM(goal13_completions) AS goal13_completions,
            SUM(goal14_completions) AS goal14_completions,
            SUM(goal15_completions) AS goal15_completions,
            SUM(goal16_completions) AS goal16_completions,
            SUM(goal17_completions) AS goal17_completions,
            SUM(goal18_completions) AS goal18_completions,
            SUM(goal19_completions) AS goal19_completions,
            SUM(goal20_completions) AS goal20_completions
        ) ga
    FROM {{ref('int_ga__performance_final')}}
    GROUP BY
        1, 2, 3, 4, 5, 6, 7
        ),

final AS (
    SELECT
        COALESCE(cr.data_source, gcr.data_source) AS data_source,
        cr.account_id,
        COALESCE(cr.account_name, gcr.account_name) AS account_name,
        cr.campaign_id,
        cr.campaign_name,
        COALESCE(cr.utm_source, gcr.utm_source) AS utm_source,
        COALESCE(cr.utm_medium, gcr.utm_medium) AS utm_medium,
        COALESCE(cr.utm_campaign, gcr.utm_campaign) AS utm_campaign,
        gcr.channel_grouping,
        cr.cost,
        cr.impressions,
        cr.clicks,
        cr.fb,
        gcr.ga,
        COALESCE(cr.date, gcr.date) AS `date`
    FROM campaign_rollup cr
    FULL OUTER JOIN ga_campaign_rollup gcr
        ON cr.utm_campaign = gcr.utm_campaign
        AND cr.utm_medium = gcr.utm_medium
        AND cr.utm_source = gcr.utm_source
        AND cr.date = gcr.date
        )

SELECT * FROM final
